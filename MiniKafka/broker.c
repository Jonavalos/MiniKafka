#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <pthread.h>
#include <fcntl.h>
#include <sys/mman.h>
#include <unistd.h>
#include <stdbool.h>
#include <signal.h>
#include <time.h>
#include <errno.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <stdarg.h>

#define LOG_MSG_SIZE 256
#define LOG_QUEUE_CAPACITY 100
#define SHM_NAME "/log_queue_shm"

#define MSG_PAYLOAD_SIZE 256
#define MSG_QUEUE_CAPACITY 100
#define MSG_SHM_NAME "/msg_queue_shm"
#define MAX_GROUP_MEMBERS 100
#define MAX_CONSUMER_OFFSETS 100

// Message structures
typedef struct {
    long long id;
    int producer_id;
    char topic[64];
    char payload[MSG_PAYLOAD_SIZE];
} Message;

typedef struct {
    pthread_mutex_t mutex;
    pthread_cond_t not_empty;
    pthread_cond_t not_full;
    int head;
    int tail;
    int count;
    volatile bool shutdown;
    long long next_message_id; // Para asignar IDs únicos
    Message messages[MSG_QUEUE_CAPACITY];
} MessageQueue;

MessageQueue *msg_queue = NULL;

// Log structures
typedef struct {
    time_t timestamp;
    char message[LOG_MSG_SIZE];
} LogMessage;

typedef struct {
    pthread_mutex_t mutex;
    pthread_cond_t not_empty;
    pthread_cond_t not_full;
    int head;
    int tail;
    int count;
    volatile bool shutdown;
    LogMessage messages[LOG_QUEUE_CAPACITY];
} LogQueue;

LogQueue *log_queue = NULL;
pthread_t logger_thread;
volatile sig_atomic_t running = 1;

// Consumer subscription structure
typedef struct {
    int consumer_id;
    int socket_fd;
    char topics[10][64];
    int topic_count;
} ConsumerSubscription;

ConsumerSubscription subscriptions[100];
int subscription_count = 0;
pthread_mutex_t subscription_mutex = PTHREAD_MUTEX_INITIALIZER;


typedef struct {
    char topic[64];
    char group_id[64];
    int consumer_id; // ID del consumidor dentro del grupo
    int socket_fd;
    // ... otra información del consumidor en el grupo ...
} GroupMember;

GroupMember group_members[MAX_GROUP_MEMBERS];
pthread_mutex_t group_members_mutex = PTHREAD_MUTEX_INITIALIZER;
int num_group_members = 0;


typedef struct {
    char topic[64];
    char group_id[64];
    int consumer_id; // ID del consumidor dentro del grupo
    long long last_consumed_offset; // ID del último mensaje consumido
    // ... otra información relacionada al offset ...
} ConsumerOffset;

ConsumerOffset consumer_offsets[MAX_CONSUMER_OFFSETS];
pthread_mutex_t consumer_offsets_mutex = PTHREAD_MUTEX_INITIALIZER;
int num_consumer_offsets = 0;


// Estructura para mantener el estado del round-robin por grupo y tema
typedef struct {
    char topic[64];
    char group_id[64];
    int next_consumer_index;
} GroupDistributionState;

GroupDistributionState group_distribution_states[100]; // Ajusta el tamaño según sea necesario
int num_group_distribution_states = 0;
pthread_mutex_t group_distribution_state_mutex = PTHREAD_MUTEX_INITIALIZER;



// Function declarations
void enqueue_log(const char *format, ...);
bool dequeue_message(Message *out_msg);
void *producer_handler_thread(void *arg);

// Subscription functions
void add_subscription(int consumer_id, int socket_fd, const char *topic, const char *group_id);
void remove_subscription(int consumer_id, const char *topic);

// Function to distribute messages to subscribed consumers
void distribute_message(const Message *msg);

// Thread para procesar mensajes y distribuirlos a los consumidores
void *message_processor_thread(void *arg);

// Manejador de señales
void signal_handler(int signum);
void cleanup_log_queue();
int init_msg_queue();
int init_log_queue();
void enqueue_message(int producer_id, const char *topic, const char *payload);
void enqueue_log(const char *format, ...);
bool dequeue_message(Message *out_msg);
void *log_writer_thread(void *arg);
void cleanup_msg_queue();
void shutdown_logger();
void *connection_handler_thread(void *arg);
void *producer_handler_thread(void *arg);
int get_next_consumer_in_group(const char *topic, const char *group_id);

// Subscription functions
void add_subscription(int consumer_id, int socket_fd, const char *topic, const char *group_id) {
    printf("Agregando suscripción antes del lock: consumidor %d, socket %d, topic %s, grupo %s\n", consumer_id, socket_fd, topic, group_id);
    pthread_mutex_lock(&subscription_mutex);
    pthread_mutex_lock(&group_members_mutex); // Proteger el acceso a group_members
    printf("Agregando suscripción en el lock: consumidor %d, socket %d, topic %s, grupo %s\n", consumer_id, socket_fd, topic, group_id);

    // Registrar la suscripción individual (para la lista de todos los suscriptores por tema)
    int sub_idx = -1;
    for (int i = 0; i < subscription_count; i++) {
        if (subscriptions[i].consumer_id == consumer_id) {
            sub_idx = i;
            break;
        }
    }
    if (sub_idx == -1 && subscription_count < 100) {
        sub_idx = subscription_count++;
        subscriptions[sub_idx].consumer_id = consumer_id;
        subscriptions[sub_idx].socket_fd = socket_fd;
        subscriptions[sub_idx].topic_count = 0;
        printf("Nueva suscripción individual: consumidor %d, socket %d, topic %s\n", consumer_id, socket_fd, topic);
    }
    if (sub_idx != -1 && subscriptions[sub_idx].topic_count < 10) {
        int topic_exists = 0;
        for (int i = 0; i < subscriptions[sub_idx].topic_count; i++) {
            if (strcmp(subscriptions[sub_idx].topics[i], topic) == 0) {
                topic_exists = 1;
                break;
            }
        }
        if (!topic_exists) {
            strcpy(subscriptions[sub_idx].topics[subscriptions[sub_idx].topic_count++], topic);
            enqueue_log("Consumidor %d se suscribió al topic '%s' (individual)", consumer_id, topic);
        }
    }

    // Registrar la pertenencia al grupo (si se proporcionó un group_id)
    if (strlen(group_id) > 0 && num_group_members < MAX_GROUP_MEMBERS) {
        group_members[num_group_members].consumer_id = consumer_id;
        group_members[num_group_members].socket_fd = socket_fd;
        strcpy(group_members[num_group_members].topic, topic);
        strcpy(group_members[num_group_members].group_id, group_id);
        num_group_members++;
        enqueue_log("Consumidor %d se unió al grupo '%s' para el topic '%s'", consumer_id, group_id, topic);
        // Add to add_subscription after joining a group
        enqueue_log("DEBUG: Consumidor %d con socket %d añadido al grupo '%s' para el topic '%s'", 
        consumer_id, socket_fd, group_id, topic);
    } else if (strlen(group_id) > 0) {
        enqueue_log("Advertencia: No se pudo unir al grupo '%s' para el topic '%s' (límite de miembros alcanzado)", group_id, topic);
    }

    pthread_mutex_unlock(&group_members_mutex);
    pthread_mutex_unlock(&subscription_mutex);
    printf("Agregando suscripción después del unlock: consumidor %d, socket %d, topic %s, grupo %s\n", consumer_id, socket_fd, topic, group_id);
    debug_print_all_subscriptions();
}

void remove_subscription(int consumer_id, const char *topic) {
    pthread_mutex_lock(&subscription_mutex);
    for (int i = 0; i < subscription_count; i++) {
        if (subscriptions[i].consumer_id == consumer_id) {
            for (int j = 0; j < subscriptions[i].topic_count; j++) {
                if (strcmp(subscriptions[i].topics[j], topic) == 0) {
                    if (j < subscriptions[i].topic_count - 1) {
                        strcpy(subscriptions[i].topics[j], subscriptions[i].topics[subscriptions[i].topic_count - 1]);
                    }
                    subscriptions[i].topic_count--;
                    enqueue_log("Consumidor %d canceló su suscripción al topic '%s'", consumer_id, topic);
                    break;
                }
            }
            break;
        }
    }
    pthread_mutex_unlock(&subscription_mutex);
}

// Function to distribute messages to subscribed consumers
void distribute_message(const Message *msg) {
    pthread_mutex_lock(&subscription_mutex);
    pthread_mutex_lock(&group_members_mutex);
    pthread_mutex_lock(&group_distribution_state_mutex);

    printf("Distributing message: Producer %d, Topic '%s', ID %lld, Payload '%s'\n",
           msg->producer_id, msg->topic, msg->id, msg->payload);

    // Track processed groups to avoid sending to the same group twice
    char processed_groups[100][64] = {0};
    int processed_group_count = 0;

    // First, identify and process unique groups for this topic
    for (int i = 0; i < num_group_members; i++) {
        if (strcmp(group_members[i].topic, msg->topic) == 0) {
            // Check if we've already processed this group
            bool already_processed = false;
            for (int j = 0; j < processed_group_count; j++) {
                if (strcmp(processed_groups[j], group_members[i].group_id) == 0) {
                    already_processed = true;
                    break;
                }
            }
            
            if (!already_processed) {
                // Get the next consumer for this group
                int receiver_fd = get_next_consumer_in_group(msg->topic, group_members[i].group_id);
                
                printf("DEBUG: For topic '%s', group '%s', selected socket: %d\n", 
                       msg->topic, group_members[i].group_id, receiver_fd);
                enqueue_log("DEBUG: Para topic '%s', grupo '%s', socket seleccionado: %d", 
                            msg->topic, group_members[i].group_id, receiver_fd);
                
                if (receiver_fd > 0) {
                    ssize_t bytes_written = write(receiver_fd, msg, sizeof(Message));
                    if (bytes_written < 0) {
                        printf("ERROR: Failed to send message to socket %d: %s\n", 
                               receiver_fd, strerror(errno));
                        enqueue_log("ERROR: Falló envío de mensaje a socket %d: %s",
                                    receiver_fd, strerror(errno));
                    } else {
                        printf("SUCCESS: Sent %zd bytes to consumer on socket %d\n", 
                               bytes_written, receiver_fd);
                        enqueue_log("SUCCESS: Enviados %zd bytes al consumidor en socket %d",
                                    bytes_written, receiver_fd);
                    }
                } else {
                    printf("ERROR: Invalid socket selected for group '%s', topic '%s'\n",
                           group_members[i].group_id, msg->topic);
                    enqueue_log("ERROR: Socket inválido seleccionado para grupo '%s', topic '%s'",
                                group_members[i].group_id, msg->topic);
                }
                
                // Mark this group as processed
                strcpy(processed_groups[processed_group_count], group_members[i].group_id);
                processed_group_count++;
            }
        }
    }

    // For individual consumers (not in groups)
    for (int i = 0; i < subscription_count; i++) {
        for (int j = 0; j < subscriptions[i].topic_count; j++) {
            if (strcmp(subscriptions[i].topics[j], msg->topic) == 0) {
                // Check if this consumer is part of a group for this topic
                bool is_in_group = false;
                for (int k = 0; k < num_group_members; k++) {
                    if (subscriptions[i].consumer_id == group_members[k].consumer_id &&
                        strcmp(msg->topic, group_members[k].topic) == 0) {
                        is_in_group = true;
                        break;
                    }
                }
                
                // Only send to individual consumers not in a group
                if (!is_in_group) {
                    ssize_t bytes_written = write(subscriptions[i].socket_fd, msg, sizeof(Message));
                    if (bytes_written < 0) {
                        enqueue_log("ERROR: Falló envío individual a socket %d: %s",
                                    subscriptions[i].socket_fd, strerror(errno));
                    } else {
                        enqueue_log("SUCCESS: Enviados %zd bytes al consumidor individual %d",
                                    bytes_written, subscriptions[i].consumer_id);
                    }
                }
                break;
            }
        }
    }

    pthread_mutex_unlock(&group_distribution_state_mutex);
    pthread_mutex_unlock(&group_members_mutex);
    pthread_mutex_unlock(&subscription_mutex);
}

// Thread para procesar mensajes y distribuirlos a los consumidores
void *message_processor_thread(void *arg) {
    Message msg;

    while (running || (msg_queue && msg_queue->count > 0)) {
        if (dequeue_message(&msg)) {
            // Log message reception
            printf("DEBUG: Dequeued message ID %lld from producer %d, topic '%s'\n", 
                   msg.id, msg.producer_id, msg.topic);
            enqueue_log("DEBUG: Dequeued message ID %lld from producer %d, topic '%s'", 
                       msg.id, msg.producer_id, msg.topic);
            
            // Use debug version of distribute
            distribute_message_debug(&msg);
        }

        // Small pause to avoid excessive CPU usage
        usleep(1000); // 1ms
    }

    return NULL;
}

// Manejador de señales
void signal_handler(int signum) {
    running = 0;
    if (log_queue) {
        log_queue->shutdown = true;
        pthread_cond_signal(&log_queue->not_empty);
    }
}

void cleanup_log_queue() {
    if (log_queue) {
        pthread_mutex_destroy(&log_queue->mutex);
        pthread_cond_destroy(&log_queue->not_empty);
        pthread_cond_destroy(&log_queue->not_full);
        munmap(log_queue, sizeof(LogQueue));
        shm_unlink(SHM_NAME);
    }
}

int init_msg_queue() {
    int fd = shm_open(MSG_SHM_NAME, O_CREAT | O_RDWR, 0666);
    if (fd == -1) {
        perror("shm_open MSG");
        return -1;
    }

    if (ftruncate(fd, sizeof(MessageQueue)) == -1) {
        perror("ftruncate MSG");
        close(fd);
        return -1;
    }

    msg_queue = mmap(NULL, sizeof(MessageQueue), PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
    close(fd);

    if (msg_queue == MAP_FAILED) {
        perror("mmap MSG");
        return -1;
    }

    pthread_mutexattr_t mattr;
    pthread_condattr_t cattr;
    pthread_mutexattr_init(&mattr);
    pthread_mutexattr_setpshared(&mattr, PTHREAD_PROCESS_SHARED);
    pthread_condattr_init(&cattr);
    pthread_condattr_setpshared(&cattr, PTHREAD_PROCESS_SHARED);

    pthread_mutex_init(&msg_queue->mutex, &mattr);
    pthread_cond_init(&msg_queue->not_empty, &cattr);
    pthread_cond_init(&msg_queue->not_full, &cattr);

    pthread_mutexattr_destroy(&mattr);
    pthread_condattr_destroy(&cattr);

    msg_queue->head = 0;
    msg_queue->tail = 0;
    msg_queue->count = 0;
    msg_queue->shutdown = false;
    msg_queue->next_message_id = 0; // Inicializar el contador de IDs

    return 0;
}

int init_log_queue() {
    int fd = shm_open(SHM_NAME, O_CREAT | O_RDWR, 0666);
    if (fd == -1) {
        perror("shm_open failed");
        return -1;
    }

    if (ftruncate(fd, sizeof(LogQueue)) == -1) {
        perror("ftruncate failed");
        close(fd);
        return -1;
    }

    log_queue = mmap(NULL, sizeof(LogQueue), PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
    close(fd);

    if (log_queue == MAP_FAILED) {
        perror("mmap failed");
        return -1;
    }

    pthread_mutexattr_t mattr;
    pthread_condattr_t cattr;

    pthread_mutexattr_init(&mattr);
    pthread_mutexattr_setpshared(&mattr, PTHREAD_PROCESS_SHARED);

    pthread_condattr_init(&cattr);
    pthread_condattr_setpshared(&cattr, PTHREAD_PROCESS_SHARED);

    pthread_mutex_init(&log_queue->mutex, &mattr);
    pthread_cond_init(&log_queue->not_empty, &cattr);
    pthread_cond_init(&log_queue->not_full, &cattr);

    pthread_mutexattr_destroy(&mattr);
    pthread_condattr_destroy(&cattr);

    log_queue->head = 0;
    log_queue->tail = 0;
    log_queue->count = 0;
    log_queue->shutdown = false;

    return 0;
}

void enqueue_message(int producer_id, const char *topic, const char *payload) {
    if (!msg_queue) return;

    pthread_mutex_lock(&msg_queue->mutex);

    while (msg_queue->count >= MSG_QUEUE_CAPACITY && !msg_queue->shutdown) {
        pthread_cond_wait(&msg_queue->not_full, &msg_queue->mutex);
    }

    if (msg_queue->shutdown) {
        pthread_mutex_unlock(&msg_queue->mutex);
        return;
    }

    Message *msg = &msg_queue->messages[msg_queue->tail];
    msg->id = msg_queue->next_message_id++; // Asignar ID único e incrementar el contador
    msg->producer_id = producer_id;
    strncpy(msg->topic, topic, sizeof(msg->topic) - 1);
    strncpy(msg->payload, payload, sizeof(msg->payload) - 1);
    msg->topic[sizeof(msg->topic) - 1] = '\0';
    msg->payload[sizeof(msg->payload) - 1] = '\0';

    msg_queue->tail = (msg_queue->tail + 1) % MSG_QUEUE_CAPACITY;
    msg_queue->count++;

    pthread_cond_signal(&msg_queue->not_empty);
    pthread_mutex_unlock(&msg_queue->mutex);
}

void enqueue_log(const char *format, ...) {
    if (!log_queue) return;

    pthread_mutex_lock(&log_queue->mutex);

    while (log_queue->count >= LOG_QUEUE_CAPACITY && !log_queue->shutdown) {
        pthread_cond_wait(&log_queue->not_full, &log_queue->mutex);
    }

    if (log_queue->shutdown) {
        pthread_mutex_unlock(&log_queue->mutex);
        return;
    }

    log_queue->messages[log_queue->tail].timestamp = time(NULL);

    va_list args;
    va_start(args, format);
    vsnprintf(log_queue->messages[log_queue->tail].message, LOG_MSG_SIZE - 1, format, args);
    va_end(args);
    log_queue->messages[log_queue->tail].message[LOG_MSG_SIZE - 1] = '\0';

    log_queue->tail = (log_queue->tail + 1) % LOG_QUEUE_CAPACITY;
    log_queue->count++;

    pthread_cond_signal(&log_queue->not_empty);
    pthread_mutex_unlock(&log_queue->mutex);
}

bool dequeue_message(Message *out_msg) {
    if (!msg_queue || !out_msg) return false;

    pthread_mutex_lock(&msg_queue->mutex);

    while (msg_queue->count == 0 && !msg_queue->shutdown) {
        pthread_cond_wait(&msg_queue->not_empty, &msg_queue->mutex);
    }

    if (msg_queue->count == 0 && msg_queue->shutdown) {
        pthread_mutex_unlock(&msg_queue->mutex);
        return false;
    }

    *out_msg = msg_queue->messages[msg_queue->head];
    msg_queue->head = (msg_queue->head + 1) % MSG_QUEUE_CAPACITY;
    msg_queue->count--;

    pthread_cond_signal(&msg_queue->not_full);
    pthread_mutex_unlock(&msg_queue->mutex);
    return true;
}

void *log_writer_thread(void *arg) {
    FILE *logfile = fopen("broker.log", "a");
    if (!logfile) {
        perror("Error opening log file");
        return NULL;
    }

    char timestamp_str[32];
    struct tm *time_info;

    while (running || (log_queue && log_queue->count > 0)) {
        pthread_mutex_lock(&log_queue->mutex);

        // Esperar hasta que haya mensajes o shutdown
        while (log_queue->count == 0 && !log_queue->shutdown) {
            pthread_cond_wait(&log_queue->not_empty, &log_queue->mutex);
        }

        // Salir si estamos en shutdown y no hay más mensajes
        if (log_queue->count == 0 && log_queue->shutdown) {
            pthread_mutex_unlock(&log_queue->mutex);
            break;
        }

        // Tomar un mensaje de la cola
        LogMessage msg = log_queue->messages[log_queue->head];
        log_queue->head = (log_queue->head + 1) % LOG_QUEUE_CAPACITY;
        log_queue->count--;

        // Señalar que hay espacio disponible
        pthread_cond_signal(&log_queue->not_full);
        pthread_mutex_unlock(&log_queue->mutex);

        // Formatear timestamp
        time_info = localtime(&msg.timestamp);
        strftime(timestamp_str, sizeof(timestamp_str), "%Y-%m-%d %H:%M:%S", time_info);

        // Escribir en el archivo de log
        fprintf(logfile, "[%s] %s\n", timestamp_str, msg.message);
        fflush(logfile);
    }

    fclose(logfile);
    return NULL;
}

void cleanup_msg_queue() {
    if (msg_queue) {
        pthread_mutex_destroy(&msg_queue->mutex);
        pthread_cond_destroy(&msg_queue->not_empty);
        pthread_cond_destroy(&msg_queue->not_full);
        munmap(msg_queue, sizeof(MessageQueue));
        shm_unlink(MSG_SHM_NAME);
    }
}

void shutdown_logger() {
    if (!log_queue) return;

    // Señalizar shutdown y esperar a que el thread termine
    pthread_mutex_lock(&log_queue->mutex);
    log_queue->shutdown = true;
    pthread_cond_signal(&log_queue->not_empty);
    pthread_mutex_unlock(&log_queue->mutex);

    // Esperar a que el thread de logging termine
    pthread_join(logger_thread, NULL);

    // Limpiar recursos
    cleanup_log_queue();
}

// Función para manejar las conexiones de clientes (productores y consumidores)
void *connection_handler_thread(void *arg) {
    int server_fd = *((int *)arg);
    struct sockaddr_in client_addr;
    socklen_t client_len = sizeof(client_addr);
    int client_fd;

    // En el thread de conexiones, configura un timeout para el socket
    struct timeval timeout;
    timeout.tv_sec = 100;
    timeout.tv_usec = 0;
    setsockopt(server_fd, SOL_SOCKET, SO_RCVTIMEO, &timeout, sizeof(timeout));

    printf("Esperando conexiones...\n");
    while (running) {
        printf("accept antes...\n");
        client_fd = accept(server_fd, (struct sockaddr *)&client_addr, &client_len);
        printf("accept despues...\n");
        if (client_fd < 0) {
            if (errno == EINTR) continue;
            if (errno == EAGAIN || errno == EWOULDBLOCK) {
                printf("accept timeout, continuando...\n");
                continue;
            }
            perror("accept failed");
            break;
        }
        printf("Cliente conectado con FD: %d\n", client_fd);
        enqueue_log("Cliente conectado con FD: %d", client_fd);

        // Leer los primeros bytes para determinar si es productor o consumidor (y su acción)
        char buffer[128];
        ssize_t bytes_received = recv(client_fd, buffer, sizeof(buffer) - 1, 0);
        if (bytes_received > 0) {
            buffer[bytes_received] = '\0';
            printf("Datos recibidos del cliente %d: '%s'\n", client_fd, buffer);
            enqueue_log("Recibidos datos del cliente %d: '%s'", client_fd, buffer);
            printf("Buffer de suscripción: '%s'\n", buffer);
            // Procesar mensaje de suscripción
            // Procesar mensaje de suscripción
            if (strncmp(buffer, "SUBSCRIBE:", 10) == 0) {
                int consumer_id;
                char topic[64] = "";
                char group_id[64] = "";
                int matches = sscanf(buffer, "SUBSCRIBE:%d:%63[^:]:%63s", &consumer_id, topic, group_id);

                printf("sscanf matches: %d, consumer_id: %d, topic: '%s', group_id: '%s'\n", matches, consumer_id, topic, group_id);
                enqueue_log("sscanf matches: %d, consumer_id: %d, topic: '%s', group_id: '%s'", matches, consumer_id, topic, group_id);

                if (matches == 3) {
                    add_subscription(consumer_id, client_fd, topic, group_id);
                    printf("Llamando a add_subscription para el consumidor %d, topic '%s', grupo '%s'\n", consumer_id, topic, group_id);
                    continue;
                } else if (matches == 2) {
                    // Si solo coinciden consumer_id y topic, asumimos no hay grupo
                    sscanf(buffer, "SUBSCRIBE:%d:%63s", &consumer_id, topic);
                    add_subscription(consumer_id, client_fd, topic, "");
                    printf("Llamando a add_subscription para el consumidor %d, topic '%s' (sin grupo)\n", consumer_id, topic);
                    continue;
                } else {
                    fprintf(stderr, "Formato de suscripción incorrecto del cliente %d: '%s'\n", client_fd, buffer);
                    enqueue_log("Formato de suscripción incorrecto del cliente %d: '%s'", client_fd, buffer);
                    close(client_fd);
                    continue;
                }
                printf("DEBUG: Subscription processed for consumer %d, socket %d\n", consumer_id, client_fd);
                enqueue_log("DEBUG: Subscription processed for consumer %d, socket %d", consumer_id, client_fd);
                // Send confirmation message back to consumer
                char confirm_msg[100];
                sprintf(confirm_msg, "SUBSCRIBED:%d:%s:%s", consumer_id, topic, group_id);
                if (write(client_fd, confirm_msg, strlen(confirm_msg)) < 0) {
                    printf("DEBUG: Failed to send confirmation to consumer: %s\n", strerror(errno));
                    enqueue_log("DEBUG: Failed to send confirmation to consumer: %s", strerror(errno));
                }
            } else {
                // Asumir que es un productor enviando un mensaje
                pthread_t producer_thread;
                int *client_socket_ptr = malloc(sizeof(int));
                if (client_socket_ptr == NULL) {
                    perror("Error al asignar memoria para el socket del cliente");
                    close(client_fd);
                    continue;
                }
                *client_socket_ptr = client_fd;
                if (pthread_create(&producer_thread, NULL, producer_handler_thread, client_socket_ptr) != 0) {
                    perror("Error al crear el hilo del productor");
                    close(client_fd);
                } else {
                    printf("Hilo del productor creado para el cliente %d\n", client_fd);
                    pthread_detach(producer_thread);
                }
            }
        } else if (bytes_received == 0) {
            printf("Cliente %d cerró la conexión.\n", client_fd);
            enqueue_log("Cliente %d cerró la conexión.", client_fd);
            close(client_fd);
        } else {
            perror("Error al recibir datos del cliente");
            enqueue_log("Error al recibir datos del cliente %d: %s", client_fd, strerror(errno));
            close(client_fd);
        }
    }
    printf("Cerrando hilo de conexión\n");
    return NULL;
}

// Función para manejar un productor conectado
void *producer_handler_thread(void *arg) {
    printf("entrando al producer handler thread \n");

    int client_fd = *((int *)arg);
    free(arg);

    Message msg;
    ssize_t bytes_read;

    while (running) {
        bytes_read = read(client_fd, &msg, sizeof(Message));
        if (bytes_read <= 0) {
            break; // Conexión cerrada o error
        }
        printf("Mensaje recibido (bytes: %zd): Producer %d, Topic '%s', Payload '%s'\n",
        bytes_read, msg.producer_id, msg.topic, msg.payload);
        if (bytes_read < 0) {
            perror("Error al leer del socket");
            break;
        } else if (bytes_read == 0) {
            break; // Conexión cerrada por el cliente
        }

        // Imprimir el mensaje recibido para verificar
        printf("Mensaje recibido del productor %d, tema: %s, payload: %s\n",
               msg.producer_id, msg.topic, msg.payload);

        // Encolar el mensaje recibido
        enqueue_message(msg.producer_id, msg.topic, msg.payload);
    }

    close(client_fd);
    return NULL;
}

// Función para obtener el siguiente consumidor en un grupo (round-robin)
int get_next_consumer_in_group(const char *topic, const char *group_id) {
    pthread_mutex_lock(&group_members_mutex);
    pthread_mutex_lock(&group_distribution_state_mutex);

    // Debug information
    printf("Finding consumer for topic: '%s', group: '%s'\n", topic, group_id);
    enqueue_log("DEBUG: Buscando consumer para topic '%s', grupo '%s'", topic, group_id);
    
    // Count group members and collect their socket_fds
    int member_count = 0;
    int socket_fds[MAX_GROUP_MEMBERS];
    int consumer_ids[MAX_GROUP_MEMBERS];
    
    for (int i = 0; i < num_group_members; i++) {
        if (strcmp(group_members[i].topic, topic) == 0 && 
            strcmp(group_members[i].group_id, group_id) == 0) {
            socket_fds[member_count] = group_members[i].socket_fd;
            consumer_ids[member_count] = group_members[i].consumer_id;
            member_count++;
        }
    }
    
    printf("Found %d members in group '%s' for topic '%s'\n", member_count, group_id, topic);
    enqueue_log("DEBUG: Encontrados %d miembros en grupo '%s' para topic '%s'", 
                member_count, group_id, topic);

    if (member_count == 0) {
        pthread_mutex_unlock(&group_distribution_state_mutex);
        pthread_mutex_unlock(&group_members_mutex);
        return -1;
    }

    // Find or create distribution state for this group/topic
    int state_index = -1;
    for (int i = 0; i < num_group_distribution_states; i++) {
        if (strcmp(group_distribution_states[i].topic, topic) == 0 &&
            strcmp(group_distribution_states[i].group_id, group_id) == 0) {
            state_index = i;
            break;
        }
    }

    if (state_index == -1) {
        if (num_group_distribution_states < 100) {
            state_index = num_group_distribution_states++;
            strcpy(group_distribution_states[state_index].topic, topic);
            strcpy(group_distribution_states[state_index].group_id, group_id);
            group_distribution_states[state_index].next_consumer_index = 0;
            printf("Created new distribution state for topic '%s', group '%s'\n", topic, group_id);
        } else {
            pthread_mutex_unlock(&group_distribution_state_mutex);
            pthread_mutex_unlock(&group_members_mutex);
            return -1;
        }
    }

    // Get current index and update it for next time
    int current_index = group_distribution_states[state_index].next_consumer_index;
    if (current_index >= member_count) {
        current_index = 0;  // Safety check
    }
    
    // Update index for next round
    group_distribution_states[state_index].next_consumer_index = (current_index + 1) % member_count;
    
    int selected_socket = socket_fds[current_index];
    int selected_consumer = consumer_ids[current_index];
    
    printf("Selected consumer %d with socket %d (index %d of %d)\n", 
           selected_consumer, selected_socket, current_index, member_count);
    enqueue_log("DEBUG: Seleccionado consumidor %d con socket %d (índice %d de %d)",
                selected_consumer, selected_socket, current_index, member_count);

    pthread_mutex_unlock(&group_distribution_state_mutex);
    pthread_mutex_unlock(&group_members_mutex);
    
    return selected_socket;
}

int get_next_consumer_in_group_debug(const char *topic, const char *group_id) {
    printf("DEBUG: get_next_consumer_in_group called for topic '%s', group '%s'\n", topic, group_id);
    enqueue_log("DEBUG: get_next_consumer_in_group called for topic '%s', group '%s'", topic, group_id);
    
    // Manual search through all group members - bypassing state tracking for now
    int matching_members[MAX_GROUP_MEMBERS];
    int matching_sockets[MAX_GROUP_MEMBERS];
    int count = 0;
    
    // Collect all members that match this topic and group
    printf("DEBUG: Searching through %d total group members\n", num_group_members);
    enqueue_log("DEBUG: Searching through %d total group members", num_group_members);
    
    for (int i = 0; i < num_group_members; i++) {
        printf("DEBUG: Checking member %d: topic='%s', group='%s', socket=%d\n", 
               i, group_members[i].topic, group_members[i].group_id, group_members[i].socket_fd);
        
        if (strcmp(group_members[i].topic, topic) == 0 && 
            strcmp(group_members[i].group_id, group_id) == 0) {
            
            matching_members[count] = group_members[i].consumer_id;
            matching_sockets[count] = group_members[i].socket_fd;
            count++;
            
            printf("DEBUG: Found matching member: consumer_id=%d, socket=%d\n", 
                   group_members[i].consumer_id, group_members[i].socket_fd);
            enqueue_log("DEBUG: Found matching member: consumer_id=%d, socket=%d", 
                       group_members[i].consumer_id, group_members[i].socket_fd);
        }
    }
    
    printf("DEBUG: Found %d matching members for topic '%s', group '%s'\n", 
           count, topic, group_id);
    enqueue_log("DEBUG: Found %d matching members for topic '%s', group '%s'", 
               count, topic, group_id);
    
    if (count == 0) {
        printf("DEBUG: No matching members found, returning -1\n");
        enqueue_log("DEBUG: No matching members found, returning -1");
        return -1;
    }
    
    // Simple round-robin: use the message counter as index
    static int counter = 0;
    int selected_index = counter % count;
    counter++;
    
    int selected_socket = matching_sockets[selected_index];
    
    printf("DEBUG: Selected socket %d (member index %d of %d)\n", 
           selected_socket, selected_index, count);
    enqueue_log("DEBUG: Selected socket %d (member index %d of %d)", 
               selected_socket, selected_index, count);
    
    return selected_socket;
}

void distribute_message_debug(const Message *msg) {
    printf("DEBUG: distribute_message_debug called for message ID %lld, topic '%s'\n", 
           msg->id, msg->topic);
    enqueue_log("DEBUG: distribute_message_debug called for message ID %lld, topic '%s'", 
               msg->id, msg->topic);
    
    // First, find all unique groups for this topic
    char unique_groups[100][64];
    int unique_group_count = 0;
    
    printf("DEBUG: Looking for groups with topic '%s' in %d total members\n", 
           msg->topic, num_group_members);
    enqueue_log("DEBUG: Looking for groups with topic '%s' in %d total members", 
               msg->topic, num_group_members);
    
    for (int i = 0; i < num_group_members; i++) {
        if (strcmp(group_members[i].topic, msg->topic) == 0) {
            // Check if we already found this group
            bool found = false;
            for (int j = 0; j < unique_group_count; j++) {
                if (strcmp(unique_groups[j], group_members[i].group_id) == 0) {
                    found = true;
                    break;
                }
            }
            
            if (!found) {
                strcpy(unique_groups[unique_group_count], group_members[i].group_id);
                unique_group_count++;
                printf("DEBUG: Found unique group '%s' for topic '%s'\n", 
                       group_members[i].group_id, msg->topic);
                enqueue_log("DEBUG: Found unique group '%s' for topic '%s'", 
                           group_members[i].group_id, msg->topic);
            }
        }
    }
    
    printf("DEBUG: Found %d unique groups for topic '%s'\n", unique_group_count, msg->topic);
    enqueue_log("DEBUG: Found %d unique groups for topic '%s'", unique_group_count, msg->topic);
    
    // For each unique group, select a consumer and send the message
    for (int i = 0; i < unique_group_count; i++) {
        int socket_fd = get_next_consumer_in_group_debug(msg->topic, unique_groups[i]);
        
        printf("DEBUG: Selected socket %d for group '%s'\n", socket_fd, unique_groups[i]);
        enqueue_log("DEBUG: Selected socket %d for group '%s'", socket_fd, unique_groups[i]);
        
        if (socket_fd > 0) {
            // Check socket status
            int error = 0;
            socklen_t len = sizeof(error);
            if (getsockopt(socket_fd, SOL_SOCKET, SO_ERROR, &error, &len) < 0 || error != 0) {
                printf("DEBUG: Socket %d is in error state: %s\n", socket_fd, strerror(error));
                enqueue_log("DEBUG: Socket %d is in error state: %s", socket_fd, strerror(error));
                continue;
            }
            
            // Try to send the message
            printf("DEBUG: Attempting to write %zu bytes to socket %d\n", sizeof(Message), socket_fd);
            enqueue_log("DEBUG: Attempting to write %zu bytes to socket %d", sizeof(Message), socket_fd);
            
            ssize_t bytes_written = write(socket_fd, msg, sizeof(Message));
            
            if (bytes_written < 0) {
                printf("DEBUG: Failed to write to socket %d: %s\n", socket_fd, strerror(errno));
                enqueue_log("DEBUG: Failed to write to socket %d: %s", socket_fd, strerror(errno));
            } else {
                printf("DEBUG: Successfully wrote %zd bytes to socket %d\n", bytes_written, socket_fd);
                enqueue_log("DEBUG: Successfully wrote %zd bytes to socket %d", bytes_written, socket_fd);
            }
        } else {
            printf("DEBUG: No valid socket found for group '%s'\n", unique_groups[i]);
            enqueue_log("DEBUG: No valid socket found for group '%s'", unique_groups[i]);
        }
    }
    
    // Handle individual subscribers not in groups
    printf("DEBUG: Checking individual subscribers for topic '%s'\n", msg->topic);
    enqueue_log("DEBUG: Checking individual subscribers for topic '%s'", msg->topic);
    
    int individual_count = 0;
    for (int i = 0; i < subscription_count; i++) {
        for (int j = 0; j < subscriptions[i].topic_count; j++) {
            if (strcmp(subscriptions[i].topics[j], msg->topic) == 0) {
                // Check if this consumer is in a group for this topic
                bool in_group = false;
                for (int k = 0; k < num_group_members; k++) {
                    if (subscriptions[i].consumer_id == group_members[k].consumer_id &&
                        strcmp(msg->topic, group_members[k].topic) == 0) {
                        in_group = true;
                        break;
                    }
                }
                
                if (!in_group) {
                    individual_count++;
                    printf("DEBUG: Sending to individual subscriber %d on socket %d\n", 
                           subscriptions[i].consumer_id, subscriptions[i].socket_fd);
                    enqueue_log("DEBUG: Sending to individual subscriber %d on socket %d", 
                               subscriptions[i].consumer_id, subscriptions[i].socket_fd);
                    
                    ssize_t bytes_written = write(subscriptions[i].socket_fd, msg, sizeof(Message));
                    
                    if (bytes_written < 0) {
                        printf("DEBUG: Failed to write to individual socket %d: %s\n", 
                               subscriptions[i].socket_fd, strerror(errno));
                        enqueue_log("DEBUG: Failed to write to individual socket %d: %s", 
                                   subscriptions[i].socket_fd, strerror(errno));
                    } else {
                        printf("DEBUG: Successfully wrote %zd bytes to individual socket %d\n", 
                               bytes_written, subscriptions[i].socket_fd);
                        enqueue_log("DEBUG: Successfully wrote %zd bytes to individual socket %d", 
                                   bytes_written, subscriptions[i].socket_fd);
                    }
                }
                break;
            }
        }
    }
    
    printf("DEBUG: Found %d individual subscribers for topic '%s'\n", individual_count, msg->topic);
    enqueue_log("DEBUG: Found %d individual subscribers for topic '%s'", individual_count, msg->topic);
}
void debug_print_all_subscriptions() {
    printf("\n----- DEBUG: ALL SUBSCRIPTIONS -----\n");
    printf("Individual subscriptions: %d\n", subscription_count);
    for (int i = 0; i < subscription_count; i++) {
        printf("Consumer %d (socket %d) topics:", 
               subscriptions[i].consumer_id, subscriptions[i].socket_fd);
        for (int j = 0; j < subscriptions[i].topic_count; j++) {
            printf(" '%s'", subscriptions[i].topics[j]);
        }
        printf("\n");
    }
    
    printf("\nGroup memberships: %d\n", num_group_members);
    for (int i = 0; i < num_group_members; i++) {
        printf("Consumer %d (socket %d): topic '%s', group '%s'\n", 
               group_members[i].consumer_id, group_members[i].socket_fd,
               group_members[i].topic, group_members[i].group_id);
    }
    printf("----- END DEBUG -----\n\n");
    
    // Also log to file
    enqueue_log("DEBUG: Printed all subscriptions to console");
}

int main() {
    printf("Iniciando broker...\n");
    enqueue_log("Broker iniciado y escuchando en el puerto 8080");
    // Configurar manejadores de señales
    signal(SIGINT, signal_handler);
    signal(SIGTERM, signal_handler);
    printf("Manejadores de señales configurados\n");

    // Inicializar la cola de logs
    if (init_log_queue() != 0) {
        fprintf(stderr, "Failed to initialize log queue\n");
        return 1;
    }
    printf("Cola de logs inicializada\n");

    // Inicializar la cola de mensajes
    if (init_msg_queue() != 0) {
        fprintf(stderr, "Failed to initialize message queue\n");
        cleanup_log_queue();
        return 1;
    }
    printf("Cola de mensajes inicializada\n");

    // Crear thread para procesar logs
    if (pthread_create(&logger_thread, NULL, log_writer_thread, NULL) != 0) {
        perror("Failed to create logger thread");
        cleanup_log_queue();
        cleanup_msg_queue();
        return 1;
    }
    printf("logger_thread creado (procesar logs)\n");

    // Crear thread para procesar mensajes
    pthread_t msg_processor_thread;
    if (pthread_create(&msg_processor_thread, NULL, message_processor_thread, NULL) != 0) {
        perror("Failed to create message processor thread");
        shutdown_logger();
        cleanup_msg_queue();
        return 1;
    }
    printf("msg_processor_thread creado (procesar mensajes)\n");

    // Configurar socket para escuchar conexiones
    int server_fd;
    struct sockaddr_in address;
    int opt = 1;
    int port = 8080;

    // Crear socket
    if ((server_fd = socket(AF_INET, SOCK_STREAM, 0)) == 0) {
        perror("socket failed");
        shutdown_logger();
        cleanup_msg_queue();
        return 1;
    }
    printf("Socket creado\n");

    // Configurar opciones del socket
    if (setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR | SO_REUSEPORT, &opt, sizeof(opt))) {
        perror("setsockopt");
        shutdown_logger();
        cleanup_msg_queue();
        return 1;
    }
    printf("Socket configurado\n");

    // Configurar dirección del socket
    address.sin_family = AF_INET;
    address.sin_addr.s_addr = INADDR_ANY;
    address.sin_port = htons(port);

    // Bindear el socket al puerto
    if (bind(server_fd, (struct sockaddr *)&address, sizeof(address)) < 0) {
        perror("bind failed");
        shutdown_logger();
        cleanup_msg_queue();
        return 1;
    }
    printf("Socket binded al puerto %d\n", port);

    // Escuchar por conexiones entrantes
    if (listen(server_fd, 10) < 0) {
        perror("listen");
        shutdown_logger();
        cleanup_msg_queue();
        return 1;
    }
    printf("Escuchando conexiones en el puerto %d\n", port);

    enqueue_log("Broker iniciado y escuchando en el puerto 8080");

    // Crear thread para manejar conexiones
    pthread_t conn_handler_thread;
    if (pthread_create(&conn_handler_thread, NULL, connection_handler_thread, &server_fd) != 0) {
        perror("Failed to create connection handler thread");
        shutdown_logger();
        cleanup_msg_queue();
        close(server_fd);
        return 1;
    }
    printf("conn_handler_thread creado (manejar conexiones)\n");

    // Esperar señal de terminación
    while (running) {
        sleep(1);
    }
    printf("sleep 1\n");

    // Cerrar todo correctamente
    enqueue_log("Iniciando apagado del broker");
    printf("Iniciando apagado del broker\n");

    // Cerrar el socket
    close(server_fd);
    printf("Socket cerrado\n");

    // Señalizar shutdown a la cola de mensajes
    pthread_mutex_lock(&msg_queue->mutex);
    msg_queue->shutdown = true;
    pthread_cond_signal(&msg_queue->not_empty);
    pthread_mutex_unlock(&msg_queue->mutex);
    printf("Senhal de shutdown enviada a la cola de mensajes\n");

    // Esperar a que los threads terminen
    pthread_join(msg_processor_thread, NULL);
    pthread_join(conn_handler_thread, NULL);
    printf("Threads de procesamiento de mensajes y conexiones terminados\n");

    shutdown_logger();
    cleanup_msg_queue();
    printf("Cola de mensajes limpiada\n");

    printf("Broker finalizado correctamente\n");
    return 0;
}