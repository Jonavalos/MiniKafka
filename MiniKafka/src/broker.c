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
#include <poll.h>
#include <netinet/in.h>
#include <asm-generic/socket.h>  // Necesario para SO_REUSEPORT en algunas configuraciones
#include <sys/time.h>  // Para struct timeval
#include <sys/socket.h>
#include <netinet/in.h>

#define LOG_MSG_SIZE 256
#define LOG_QUEUE_CAPACITY 100
#define SHM_NAME "/log_queue_shm"

#define MSG_PAYLOAD_SIZE 256
#define MSG_QUEUE_CAPACITY 100
#define MSG_SHM_NAME "/msg_queue_shm"
#define MAX_GROUP_MEMBERS 100
#define MAX_CONSUMER_OFFSETS 100
#define MAX_MESSAGE_LENGTH 256


// Message structures
#pragma pack(push, 1)
typedef struct {
    long long id;
    int producer_id;
    char topic[64];
    char payload[256];
} Message;
#pragma pack(pop)



// Maximum message history to keep in memory (for demo purposes) OFFSET
#define MAX_MESSAGE_HISTORY 1000
Message message_history[MAX_MESSAGE_HISTORY];
int message_history_count = 0;
pthread_mutex_t message_history_mutex = PTHREAD_MUTEX_INITIALIZER;


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

typedef struct Subscription {
    int consumer_id;
    int socket_fd;
    char topics[10][64];
    int topic_count;
    struct Subscription *next; // Puntero al siguiente nodo
} ConsumerSubscription;

ConsumerSubscription *subscriptions = NULL; // Lista enlazada de suscripciones
pthread_mutex_t subscription_mutex = PTHREAD_MUTEX_INITIALIZER;

// // Consumer subscription structure
// typedef struct {
//     int consumer_id;
//     int socket_fd;
//     char topics[10][64];
//     int topic_count;
// } ConsumerSubscription;

// ConsumerSubscription subscriptions[100];
// int subscription_count = 0;
// pthread_mutex_t subscription_mutex = PTHREAD_MUTEX_INITIALIZER;


// typedef struct {
//     char topic[64];
//     char group_id[64];
//     int consumer_id; // ID del consumidor dentro del grupo
//     int socket_fd;
//     // ... otra información del consumidor en el grupo ...
// } GroupMember;

// GroupMember group_members[MAX_GROUP_MEMBERS];
// pthread_mutex_t group_members_mutex = PTHREAD_MUTEX_INITIALIZER;
// int num_group_members = 0;


typedef struct ConsumerOffset {
    char topic[64];
    char group_id[64];
    int consumer_id;
    long long last_consumed_offset;
    struct ConsumerOffset *next; // Puntero al siguiente nodo
} ConsumerOffset;

ConsumerOffset *consumer_offsets = NULL; // Lista enlazada de offsets
pthread_mutex_t consumer_offsets_mutex = PTHREAD_MUTEX_INITIALIZER;

// typedef struct {
//     char topic[64];
//     char group_id[64];
//     int consumer_id; // ID del consumidor dentro del grupo
//     long long last_consumed_offset; // ID del último mensaje consumido
//     // ... otra información relacionada al offset ...
// } ConsumerOffset;

// ConsumerOffset consumer_offsets[MAX_CONSUMER_OFFSETS];
// pthread_mutex_t consumer_offsets_mutex = PTHREAD_MUTEX_INITIALIZER;
// int num_consumer_offsets = 0;


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
void distribute_message(const char *topic, const char *group_id, const char *message);

// Thread para procesar mensajes y distribuirlos a los consumidores
void *message_processor_thread(void *arg);

// Manejador de señales
void signal_handler(int signum);
void cleanup_log_queue();
int init_msg_queue();
int init_log_queue();
void enqueue_message(int producer_id, const char *topic, const char *payload);
void enqueue_log(const char *format, ...);
// bool dequeue_message(Message *out_msg);
void *log_writer_thread(void *arg);
void cleanup_msg_queue();
void shutdown_logger();
void *connection_handler_thread(void *arg);
void *producer_handler_thread(void *arg);
int get_next_consumer_in_group(const char *topic, const char *group_id);
void save_consumer_offsets();
void send_messages_from_offset(int consumer_id, int client_fd, const char *topic, const char *group_id, long long start_offset);
void store_message_in_history(const Message *msg);
long long get_last_consumer_offset(const char *topic, const char *group_id, int consumer_id);


typedef struct GroupMember {
    char topic[64];
    char group_id[64];
    int consumer_id;
    int socket_fd;
    struct GroupMember *next; // Puntero al siguiente nodo
} GroupMember;

GroupMember *group_members = NULL; // Lista enlazada de miembros de grupo
pthread_mutex_t group_members_mutex = PTHREAD_MUTEX_INITIALIZER;

// Subscription functions

//************************* G R O U P    M E M B E R S***************** */
void add_group_member(const char *topic, const char *group_id, int consumer_id, int socket_fd) {
    pthread_mutex_lock(&group_members_mutex);

    // Crear un nuevo nodo para el miembro del grupo
    GroupMember *new_member = (GroupMember *)malloc(sizeof(GroupMember));
    if (!new_member) {
        perror("Error allocating memory for group member");
        pthread_mutex_unlock(&group_members_mutex);
        return;
    }

    // Inicializar los valores del nuevo miembro
    strncpy(new_member->topic, topic, sizeof(new_member->topic) - 1);
    strncpy(new_member->group_id, group_id, sizeof(new_member->group_id) - 1);
    new_member->consumer_id = consumer_id;
    new_member->socket_fd = socket_fd;
    new_member->next = group_members;

    // Agregar el nuevo miembro al inicio de la lista
    group_members = new_member;

    pthread_mutex_unlock(&group_members_mutex);
}


void remove_group_member(const char *topic, const char *group_id, int consumer_id) {
    pthread_mutex_lock(&group_members_mutex);

    GroupMember *current = group_members;
    GroupMember *prev = NULL;

    while (current != NULL) {
        if (strcmp(current->topic, topic) == 0 &&
            strcmp(current->group_id, group_id) == 0 &&
            current->consumer_id == consumer_id) {
            // Eliminar el nodo actual
            if (prev == NULL) {
                group_members = current->next;
            } else {
                prev->next = current->next;
            }
            free(current);
            enqueue_log("Removed group member: topic='%s', group='%s', consumer_id=%d",
                        topic, group_id, consumer_id);
            pthread_mutex_unlock(&group_members_mutex);
            return;
        }
        prev = current;
        current = current->next;
    }

    pthread_mutex_unlock(&group_members_mutex);
}
//************************* F I N    G R O U P    M E M B E R S***************** */


//************************* S U B S C R I P T I O N S ***************** */

void add_subscription(int consumer_id, int socket_fd, const char *topic, const char *group_id) {
    pthread_mutex_lock(&subscription_mutex);

    // Buscar si ya existe una suscripción para este consumidor
    ConsumerSubscription *current = subscriptions;
    ConsumerSubscription *prev = NULL;
    while (current != NULL) {
        if (current->consumer_id == consumer_id) {
            break;
        }
        prev = current;
        current = current->next;
    }

    // Si no existe, crear una nueva suscripción
    if (current == NULL) {
        current = (ConsumerSubscription *)malloc(sizeof(ConsumerSubscription));
        if (!current) {
            perror("Error allocating memory for subscription");
            pthread_mutex_unlock(&subscription_mutex);
            return;
        }
        current->consumer_id = consumer_id;
        current->socket_fd = socket_fd;
        current->topic_count = 0;
        current->next = NULL;

        if (prev == NULL) {
            subscriptions = current; // Primera suscripción
        } else {
            prev->next = current; // Agregar al final de la lista
        }
    }

    // Agregar el topic si no existe
    int topic_exists = 0;
    for (int i = 0; i < current->topic_count; i++) {
        if (strcmp(current->topics[i], topic) == 0) {
            topic_exists = 1;
            break;
        }
    }
    if (!topic_exists && current->topic_count < 10) {
        strcpy(current->topics[current->topic_count++], topic);
        enqueue_log("Consumer %d subscribed to topic '%s'", consumer_id, topic);
    }

    // Registrar al consumidor en el grupo
    add_group_member(topic, group_id, consumer_id, socket_fd);

    pthread_mutex_unlock(&subscription_mutex);
}

void remove_subscription(int consumer_id, const char *topic) {
    pthread_mutex_lock(&subscription_mutex);

    ConsumerSubscription *current = subscriptions;
    ConsumerSubscription *prev = NULL;

    while (current != NULL) {
        if (current->consumer_id == consumer_id) {
            // Eliminar el topic de la suscripción
            for (int i = 0; i < current->topic_count; i++) {
                if (strcmp(current->topics[i], topic) == 0) {
                    // Reemplazar el topic eliminado por el último
                    if (i < current->topic_count - 1) {
                        strcpy(current->topics[i], current->topics[current->topic_count - 1]);
                    }
                    current->topic_count--;
                    enqueue_log("Consumer %d unsubscribed from topic '%s'", consumer_id, topic);
                    break;
                }
            }

            // Si no quedan topics, eliminar la suscripción completa
            if (current->topic_count == 0) {
                if (prev == NULL) {
                    subscriptions = current->next;
                } else {
                    prev->next = current->next;
                }
                free(current);
                break;
            }
        }
        prev = current;
        current = current->next;
    }

    pthread_mutex_unlock(&subscription_mutex);
}

//************************* F I N   S U B S C R I P T I O N S ***************** */



// Función para obtener el siguiente consumidor en un grupo (round-robin)


// Helper: busca el consumer_id a partir del socket y el topic/grupo
//METER EL CONSUMER ID EN EL LOG QUEUE
static int find_consumer_id_by_socket(int sock, const char *topic, const char *group_id) {
    int cid = -1;
    pthread_mutex_lock(&group_members_mutex);

    GroupMember *current = group_members;
    while (current != NULL) {
        if (current->socket_fd == sock &&
            strcmp(current->topic, topic) == 0 &&
            strcmp(current->group_id, group_id) == 0) {
            cid = current->consumer_id;
            break;
        }
        current = current->next;
    }

    pthread_mutex_unlock(&group_members_mutex);
    return cid;
}

int get_next_consumer_in_group(const char *topic, const char *group_id) {
    pthread_mutex_lock(&group_members_mutex);
    pthread_mutex_lock(&group_distribution_state_mutex);

    printf("DEBUG: Buscando consumer para topic '%s', grupo '%s'\n", topic, group_id);
    enqueue_log("DEBUG: Buscando consumer para topic '%s', grupo '%s'", topic, group_id);

    // Contar los miembros del grupo y almacenar sus sockets e IDs
    int member_count = 0;
    int socket_fds[MAX_GROUP_MEMBERS];
    int consumer_ids[MAX_GROUP_MEMBERS];

    GroupMember *current = group_members;
    while (current != NULL) {
        if (strcmp(current->topic, topic) == 0 &&
            strcmp(current->group_id, group_id) == 0 &&
            current->socket_fd >= 0) {  // Validar socket
            socket_fds[member_count] = current->socket_fd;
            consumer_ids[member_count] = current->consumer_id;
            member_count++;
        }
        current = current->next;
    }

    if (member_count == 0) {
        printf("WARNING: No hay miembros en el grupo '%s' para el tópico '%s'\n", group_id, topic);
        enqueue_log("WARNING: No hay miembros en el grupo '%s' para el tópico '%s'", group_id, topic);
        pthread_mutex_unlock(&group_distribution_state_mutex);
        pthread_mutex_unlock(&group_members_mutex);
        return -1;
    }

    // Buscar o crear el estado del grupo
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
            printf("DEBUG: Estado de distribución creado para topic '%s', grupo '%s'\n", topic, group_id);
        } else {
            printf("ERROR: Límite de estados de distribución alcanzado\n");
            pthread_mutex_unlock(&group_distribution_state_mutex);
            pthread_mutex_unlock(&group_members_mutex);
            return -1;
        }
    }

    int current_index = group_distribution_states[state_index].next_consumer_index;

    // Validación del índice
    if (current_index >= member_count || current_index < 0) {
        printf("WARNING: Índice fuera de rango (%d), reiniciando\n", current_index);
        enqueue_log("WARNING: Índice fuera de rango (%d), reiniciando", current_index);
        current_index = 0;
        group_distribution_states[state_index].next_consumer_index = 1 % member_count;
    } else {
        group_distribution_states[state_index].next_consumer_index = (current_index + 1) % member_count;
    }

    int selected_socket = socket_fds[current_index];
    int selected_consumer = consumer_ids[current_index];

    printf("DEBUG: Seleccionado consumidor %d con socket %d (índice %d de %d)\n",
           selected_consumer, selected_socket, current_index, member_count);
    enqueue_log("DEBUG: Seleccionado consumidor %d con socket %d (índice %d de %d)",
                selected_consumer, selected_socket, current_index, member_count);

    pthread_mutex_unlock(&group_distribution_state_mutex);
    pthread_mutex_unlock(&group_members_mutex);

    return selected_socket;
}

void *message_processor_thread(void *arg) {
    (void)arg; // Evitar advertencia de parámetro no utilizado
    Message msg;

    while (running || (msg_queue && msg_queue->count > 0)) {
        if (dequeue_message(&msg)) {
            // Log del mensaje dequeued
            printf("DEBUG: Dequeued message ID %lld from producer %d, topic '%s'\n", 
                   msg.id, msg.producer_id, msg.topic);
            enqueue_log("DEBUG: Dequeued message ID %lld from producer %d, topic '%s'", 
                       msg.id, msg.producer_id, msg.topic);

            // Determinar el grupo correspondiente al mensaje
            char group_id[64] = "";
            GroupMember *current = group_members;

            pthread_mutex_lock(&group_members_mutex);
            while (current != NULL) {
                if (strcmp(current->topic, msg.topic) == 0) {
                    strncpy(group_id, current->group_id, sizeof(group_id) - 1);
                    break;
                }
                current = current->next;
            }
            pthread_mutex_unlock(&group_members_mutex);

            if (strlen(group_id) == 0) {
                printf("WARNING: No se encontró un grupo para el tópico '%s'\n", msg.topic);
                enqueue_log("WARNING: No se encontró un grupo para el tópico '%s'", msg.topic);
                continue;
            }

            // Enviar mensaje al grupo correspondiente
            distribute_message(msg.topic, group_id, msg.payload);
        }

        // Evitar uso excesivo de CPU
        usleep(1000); // 1 ms
    }

    return NULL;
}

void distribute_message(const char *topic, const char *group_id, const char *message) {
    printf("DEBUG: Intentando distribuir mensaje para tópico '%s', grupo '%s'\n", topic, group_id);
    enqueue_log("DEBUG: Intentando distribuir mensaje para tópico '%s', grupo '%s'", topic, group_id);

    char full_message[MAX_MESSAGE_LENGTH];
    snprintf(full_message, sizeof(full_message), "%s", message);

    while (true) {
        int consumer_socket = get_next_consumer_in_group(topic, group_id);
        if (consumer_socket < 0) {
            // No quedan consumidores
            enqueue_log("WARNING: No quedan consumidores para tópico '%s', grupo '%s'; mensaje descartado",
                        topic, group_id);
            return;
        }

        // Detectar hang‑up antes de enviar
        struct pollfd pfd = { .fd = consumer_socket, .events = POLLIN | POLLHUP };
        int polled = poll(&pfd, 1, 0);
        if (polled > 0 && (pfd.revents & POLLHUP)) {
            // Este consumidor murió: lo quitamos y reintentamos
            int cid = find_consumer_id_by_socket(consumer_socket, topic, group_id);
            if (cid >= 0) {
                enqueue_log("DEBUG: Consumidor %d detectado colgado antes de enviar; removiendo",
                            cid);
                remove_subscription(cid, topic);
            }
            close(consumer_socket);
            continue;  // volver a buscar siguiente consumidor
        }

        // Intentar el envío
        ssize_t bytes_sent = send(consumer_socket, full_message, strlen(full_message), MSG_NOSIGNAL);
        if (bytes_sent > 0) {
            // Obtener el ID del consumidor
            int consumer_id = find_consumer_id_by_socket(consumer_socket, topic, group_id);

            // Éxito: registrar en el log y salir del loop
            printf("DEBUG: Mensaje enviado a socket %d (consumer %d): %s\n", consumer_socket, consumer_id, full_message);
            enqueue_log("DEBUG: Mensaje enviado a socket %d (consumer %d): %s", consumer_socket, consumer_id, full_message);
            return;
        } else {
            // Error en el envío: quitamos al consumidor y reintentamos
            int cid = find_consumer_id_by_socket(consumer_socket, topic, group_id);
            if (cid >= 0) {
                enqueue_log("ERROR: Falló envío a consumidor %d; errno=%d; removiendo",
                            cid, errno);
                remove_subscription(cid, topic);
            }
            close(consumer_socket);
            // y volvemos a intentar con el siguiente
            continue;
        }
    }
}



// Manejador de señales
void signal_handler(int signum) {
    running = 0;
    save_consumer_offsets();
    if (log_queue) {
        save_consumer_offsets();
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
// Modify the connection_handler_thread function to handle subscription with offset
void *connection_handler_thread(void *arg) {
    int server_fd = *((int *)arg);
    struct sockaddr_in client_addr;
    socklen_t client_len = sizeof(client_addr);
    int client_fd;

    // Set socket timeout
    struct timeval timeout;
    timeout.tv_sec = 100;
    timeout.tv_usec = 0;
    setsockopt(server_fd, SOL_SOCKET, SO_RCVTIMEO, &timeout, sizeof(timeout));

    printf("Waiting for connections...\n");
    while (running) {
        client_fd = accept(server_fd, (struct sockaddr *)&client_addr, &client_len);
        if (client_fd < 0) {
            if (errno == EINTR) continue;
            if (errno == EAGAIN || errno == EWOULDBLOCK) {
                printf("accept timeout, continuing...\n");
                continue;
            }
            perror("accept failed");
            break;
        }
        printf("Client connected with FD: %d\n", client_fd);
        enqueue_log("Client connected with FD: %d", client_fd);

        // Read first bytes to determine if producer or consumer (and action)
        char buffer[256]; // Increased buffer size to handle offset
        ssize_t bytes_received = recv(client_fd, buffer, sizeof(buffer) - 1, 0);
        if (bytes_received > 0) {
            buffer[bytes_received] = '\0';
            printf("Data received from client %d: '%s'\n", client_fd, buffer);
            
            // Process subscription message
            if (strncmp(buffer, "SUBSCRIBE:", 10) == 0) {
                int consumer_id;
                char topic[64] = "";
                char group_id[64] = "";
                long long start_offset = -1;
                
                // Try to parse with offset
                int matches = sscanf(buffer, "SUBSCRIBE:%d:%63[^:]:%63[^:]:%lld", 
                                    &consumer_id, topic, group_id, &start_offset);
                
                if (matches == 4) {
                    // Subscription with offset and group
                    add_subscription(consumer_id, client_fd, topic, group_id);
                    
                    // If this is a specific offset, send messages since that offset
                    if (start_offset >= 0) {
                        send_messages_from_offset(consumer_id, client_fd, topic, group_id, start_offset);
                    }
                } else {
                    // Try without offset
                    matches = sscanf(buffer, "SUBSCRIBE:%d:%63[^:]:%63s", 
                                    &consumer_id, topic, group_id);
                    if (matches == 3) {
                        // Subscription with group but no offset
                        add_subscription(consumer_id, client_fd, topic, group_id);
                    } else {
                        // Try just topic
                        matches = sscanf(buffer, "SUBSCRIBE:%d:%63s", &consumer_id, topic);
                        if (matches == 2) {
                            // Subscription with no group or offset
                            add_subscription(consumer_id, client_fd, topic, "");
                        } else {
                            fprintf(stderr, "Invalid subscription format from client %d: '%s'\n", 
                                   client_fd, buffer);
                            close(client_fd);
                            continue;
                        }
                    }
                }
                
                // Send confirmation back to consumer
                char confirm_msg[100];
                snprintf(confirm_msg, sizeof(confirm_msg), "SUBSCRIBED:%d:%s:%s", consumer_id, topic, group_id);                write(client_fd, confirm_msg, strlen(confirm_msg));
            } 
            else if (strncmp(buffer, "UNSUBSCRIBE:", 12) == 0) {
                printf("Unsubscribe request from client\n");
                int consumer_id;
                char topic[64] = "";
                
                // Parse the unsubscribe message
                int matches = sscanf(buffer, "UNSUBSCRIBE:%d:%63s", &consumer_id, topic);
                
                if (matches == 2) {
                    printf("Removing subscription for consumer %d from topic '%s'\n", consumer_id, topic);
                    enqueue_log("Removing subscription for consumer %d from topic '%s'", consumer_id, topic);
                    
                    // Remove the subscription
                    printf("Removing subscription for consumer %d from topic '%s'\n", consumer_id, topic);
                    remove_subscription(consumer_id, topic);
                    
                    // Send confirmation back to consumer
                    char confirm_msg[100];
                    sprintf(confirm_msg, "UNSUBSCRIBED:%d:%s", consumer_id, topic);
                    write(client_fd, confirm_msg, strlen(confirm_msg));
                    
                    // Close the connection
                    close(client_fd);
                } else {
                    fprintf(stderr, "Invalid unsubscription format from client %d: '%s'\n", 
                           client_fd, buffer);
                    close(client_fd);
                }
            }else {
                // Assume it's a producer
                pthread_t producer_thread;
                int *client_socket_ptr = malloc(sizeof(int));
                if (client_socket_ptr == NULL) {
                    perror("Error allocating memory for client socket");
                    close(client_fd);
                    continue;
                }
                *client_socket_ptr = client_fd;
                if (pthread_create(&producer_thread, NULL, producer_handler_thread, client_socket_ptr) != 0) {
                    perror("Error creating producer thread");
                    close(client_fd);
                    free(client_socket_ptr);
                } else {
                    printf("Producer thread created for client %d\n", client_fd);
                    pthread_detach(producer_thread);
                }
            }
        } else {
            // Error or connection closed
            if (bytes_received == 0) {
                printf("Client %d closed connection.\n", client_fd);
            } else {
                perror("Error receiving data from client");
            }
            close(client_fd);
        }
    }
    printf("Closing connection thread\n");
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
//************************* O F F S E T S ***************** */


// Store the offset for a consumer group
void update_consumer_offset(const char *topic, const char *group_id, int consumer_id, long long message_id) {
    pthread_mutex_lock(&consumer_offsets_mutex);

    // Buscar el offset existente
    ConsumerOffset *current = consumer_offsets;
    ConsumerOffset *prev = NULL;

    while (current != NULL) {
        if (strcmp(current->topic, topic) == 0 &&
            strcmp(current->group_id, group_id) == 0 &&
            current->consumer_id == consumer_id) {
            // Actualizar el offset existente
            current->last_consumed_offset = message_id;
            enqueue_log("Updated offset for group '%s', topic '%s', consumer %d to %lld",
                        group_id, topic, consumer_id, message_id);
            pthread_mutex_unlock(&consumer_offsets_mutex);
            return;
        }
        prev = current;
        current = current->next;
    }

    // Si no se encontró, crear un nuevo nodo
    ConsumerOffset *new_offset = (ConsumerOffset *)malloc(sizeof(ConsumerOffset));
    if (!new_offset) {
        perror("Error allocating memory for consumer offset");
        pthread_mutex_unlock(&consumer_offsets_mutex);
        return;
    }

    strncpy(new_offset->topic, topic, sizeof(new_offset->topic) - 1);
    strncpy(new_offset->group_id, group_id, sizeof(new_offset->group_id) - 1);
    new_offset->consumer_id = consumer_id;
    new_offset->last_consumed_offset = message_id;
    new_offset->next = NULL;

    // Agregar el nuevo nodo al final de la lista
    if (prev == NULL) {
        consumer_offsets = new_offset; // Primera entrada
    } else {
        prev->next = new_offset;
    }

    enqueue_log("Created new offset for group '%s', topic '%s', consumer %d to %lld",
                group_id, topic, consumer_id, message_id);

    pthread_mutex_unlock(&consumer_offsets_mutex);
}

// Get the last consumed offset for a consumer group
long long get_last_consumer_offset(const char *topic, const char *group_id, int consumer_id) {
    pthread_mutex_lock(&consumer_offsets_mutex);

    ConsumerOffset *current = consumer_offsets;
    while (current != NULL) {
        if (strcmp(current->topic, topic) == 0 &&
            strcmp(current->group_id, group_id) == 0 &&
            current->consumer_id == consumer_id) {
            pthread_mutex_unlock(&consumer_offsets_mutex);
            return current->last_consumed_offset;
        }
        current = current->next;
    }

    pthread_mutex_unlock(&consumer_offsets_mutex);
    return -1; // No se encontró el offset
}

// Get the last consumed offset for an entire consumer group
long long get_group_last_offset(const char *topic, const char *group_id) {
    pthread_mutex_lock(&consumer_offsets_mutex);

    long long max_offset = -1;
    ConsumerOffset *current = consumer_offsets;

    while (current != NULL) {
        if (strcmp(current->topic, topic) == 0 &&
            strcmp(current->group_id, group_id) == 0) {
            if (current->last_consumed_offset > max_offset) {
                max_offset = current->last_consumed_offset;
            }
        }
        current = current->next;
    }

    pthread_mutex_unlock(&consumer_offsets_mutex);
    return max_offset;
}

// Function to save consumer offsets periodically or on shutdown
void save_consumer_offsets() {
    FILE *offset_file = fopen("consumer_offsets.dat", "w");
    if (!offset_file) {
        enqueue_log("ERROR: Failed to open consumer_offsets.dat for writing: %s",
                    strerror(errno));
        return;
    }

    pthread_mutex_lock(&consumer_offsets_mutex);

    ConsumerOffset *current = consumer_offsets;
    while (current != NULL) {
        fprintf(offset_file, "%s,%s,%d,%lld\n",
                current->topic,
                current->group_id,
                current->consumer_id,
                current->last_consumed_offset);
        current = current->next;
    }

    pthread_mutex_unlock(&consumer_offsets_mutex);
    fclose(offset_file);
    enqueue_log("Consumer offsets saved to file");
}

// Function to load consumer offsets on startup
void load_consumer_offsets() {
    FILE *offset_file = fopen("consumer_offsets.dat", "r");
    if (!offset_file) {
        if (errno != ENOENT) {
            enqueue_log("WARNING: Failed to open consumer_offsets.dat for reading: %s",
                        strerror(errno));
        }
        return;
    }

    pthread_mutex_lock(&consumer_offsets_mutex);

    // Limpiar la lista enlazada existente
    ConsumerOffset *current = consumer_offsets;
    while (current != NULL) {
        ConsumerOffset *temp = current;
        current = current->next;
        free(temp);
    }
    consumer_offsets = NULL;

    // Leer los offsets del archivo
    char line[256];
    while (fgets(line, sizeof(line), offset_file)) {
        char topic[64];
        char group_id[64];
        int consumer_id;
        long long offset;

        if (sscanf(line, "%63[^,],%63[^,],%d,%lld",
                   topic, group_id, &consumer_id, &offset) == 4) {
            update_consumer_offset(topic, group_id, consumer_id, offset);
        }
    }

    pthread_mutex_unlock(&consumer_offsets_mutex);
    fclose(offset_file);
    enqueue_log("Loaded consumer offsets from file");
}

// Function to send messages from a specific offset
void send_messages_from_offset(int consumer_id, int client_fd, const char *topic, const char *group_id, long long start_offset) {
    pthread_mutex_lock(&message_history_mutex);
    
    int count_sent = 0;
    for (int i = 0; i < message_history_count; i++) {
        if (strcmp(message_history[i].topic, topic) == 0 && 
            message_history[i].id >= start_offset) {
            
            // Send this historical message
            if (write(client_fd, &message_history[i], sizeof(Message)) > 0) {
                count_sent++;
            }
        }
    }
    
    pthread_mutex_unlock(&message_history_mutex);
    
    enqueue_log("Sent %d historical messages to consumer %d for topic '%s' from offset %lld",
               count_sent, consumer_id, topic, start_offset);
}


//************************* F I N  O F F S E T S ***************** */


// Function to store messages in history
void store_message_in_history(const Message *msg) {
    pthread_mutex_lock(&message_history_mutex);
    
    // Simple circular buffer implementation
    if (message_history_count < MAX_MESSAGE_HISTORY) {
        message_history[message_history_count++] = *msg;
    } else {
        // Shift array to make room for new message
        for (int i = 0; i < MAX_MESSAGE_HISTORY - 1; i++) {
            message_history[i] = message_history[i + 1];
        }
        message_history[MAX_MESSAGE_HISTORY - 1] = *msg;
    }
    
    pthread_mutex_unlock(&message_history_mutex);
}

// Modify the dequeue_message function to store messages in history
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
    
    // Store the message in history
    store_message_in_history(out_msg);
    
    return true;
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


    load_consumer_offsets();

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