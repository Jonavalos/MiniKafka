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

// Message structures
typedef struct {
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

// Function declarations
void enqueue_log(const char *format, ...);
bool dequeue_message(Message *out_msg);
void *producer_handler_thread(void *arg);

// Subscription functions
void add_subscription(int consumer_id, int socket_fd, const char *topic) {
    printf("Agregando suscripción antes del lock: consumidor %d, socket %d, topic %s\n", consumer_id, socket_fd, topic);
    pthread_mutex_lock(&subscription_mutex);
    printf("Agregando suscripción en el lock: consumidor %d, socket %d, topic %s\n", consumer_id, socket_fd, topic);
    int idx = -1;
    for (int i = 0; i < subscription_count; i++) {
        printf("!!!!!!!!!!!Consumer %d, topic %s\n", subscriptions[i].consumer_id, subscriptions[i].topics[0]);
        if (subscriptions[i].consumer_id == consumer_id) {
            idx = i;
            break;
        }
    }
    if (idx == -1 && subscription_count < 100) {
        idx = subscription_count++;
        subscriptions[idx].consumer_id = consumer_id;
        subscriptions[idx].socket_fd = socket_fd;
        subscriptions[idx].topic_count = 0;
        printf("Nueva suscripción: consumidor %d, socket %d, topic %s\n", consumer_id, socket_fd, topic);
    }
    if (idx != -1 && subscriptions[idx].topic_count < 10) {
        for (int i = 0; i < subscriptions[idx].topic_count; i++) {
            if (strcmp(subscriptions[idx].topics[i], topic) == 0) {
                pthread_mutex_unlock(&subscription_mutex);
                return;
            }
        }
        strcpy(subscriptions[idx].topics[subscriptions[idx].topic_count++], topic);
        char log_buffer[256];
        enqueue_log("Consumidor %d se suscribió al topic '%s'", consumer_id, topic);
    }
    pthread_mutex_unlock(&subscription_mutex);
    printf("Agregando suscripción después del unlock: consumidor %d, socket %d, topic %s\n", consumer_id, socket_fd, topic);
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
                    char log_buffer[256];
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
    printf("Distributing message: Producer %d, Topic '%s', Payload '%s'\n", msg->producer_id, msg->topic, msg->payload);
    for (int i = 0; i < subscription_count; i++) {
        printf("Consumer %d has %d subscriptions:\n", subscriptions[i].consumer_id, subscriptions[i].topic_count);
        for (int j = 0; j < subscriptions[i].topic_count; j++) {
            printf("  - '%s'\n", subscriptions[i].topics[j]);
            if (strcmp(subscriptions[i].topics[j], msg->topic) == 0) {
                if (write(subscriptions[i].socket_fd, msg, sizeof(Message)) < 0) {
                    enqueue_log("Error al enviar mensaje (productor %d, topic '%s') al consumidor %d: %s",
                                msg->producer_id, msg->topic, subscriptions[i].consumer_id, strerror(errno));
                } else {
                    enqueue_log("Mensaje (productor %d, topic '%s') enviado al consumidor %d",
                                msg->producer_id, msg->topic, subscriptions[i].consumer_id);
                }
                break;
            }
        }
    }
    pthread_mutex_unlock(&subscription_mutex);
}

// Thread para procesar mensajes y distribuirlos a los consumidores
void *message_processor_thread(void *arg) {
    Message msg;

    while (running || (msg_queue && msg_queue->count > 0)) {
        if (dequeue_message(&msg)) {
            // Registrar recepción del mensaje CON el contenido
            enqueue_log("Mensaje recibido del productor %d con topic '%s', payload: '%s'",
                        msg.producer_id, msg.topic, msg.payload);

            // Distribuir el mensaje a los consumidores suscritos
            distribute_message(&msg);
        }

        // Pequeña pausa para evitar consumo excesivo de CPU
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
        // Destruir los objetos de sincronización
        pthread_mutex_destroy(&log_queue->mutex);
        pthread_cond_destroy(&log_queue->not_empty);
        pthread_cond_destroy(&log_queue->not_full);
        
        // Desasociar la memoria compartida
        munmap(log_queue, sizeof(LogQueue));
        
        // Eliminar el objeto de memoria compartida
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



// Función para añadir logs a la cola con reintentos
void enqueue_log(const char *format, ...) {
    if (!log_queue) return;

    pthread_mutex_lock(&log_queue->mutex);

    // Esperar hasta que haya espacio disponible o shutdown
    while (log_queue->count >= LOG_QUEUE_CAPACITY && !log_queue->shutdown) {
        pthread_cond_wait(&log_queue->not_full, &log_queue->mutex);
    }

    // Salir si estamos en shutdown
    if (log_queue->shutdown) {
        pthread_mutex_unlock(&log_queue->mutex);
        return;
    }

    // Añadir timestamp actual
    log_queue->messages[log_queue->tail].timestamp = time(NULL);

    // Formatear el mensaje de log con argumentos variables
    va_list args;
    va_start(args, format);
    vsnprintf(log_queue->messages[log_queue->tail].message, LOG_MSG_SIZE - 1, format, args);
    va_end(args);
    log_queue->messages[log_queue->tail].message[LOG_MSG_SIZE - 1] = '\0';

    log_queue->tail = (log_queue->tail + 1) % LOG_QUEUE_CAPACITY;
    log_queue->count++;

    // Notificar que hay un nuevo mensaje
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

            // Procesar mensaje de suscripción
            if (strncmp(buffer, "SUBSCRIBE:", 10) == 0) {
                int consumer_id;
                char topic[64];
                if (sscanf(buffer, "SUBSCRIBE:%d:%63s", &consumer_id, topic) == 2) {
                    add_subscription(consumer_id, client_fd, topic);
                    printf("Llamando a add_subscription para el consumidor %d, topic '%s'\n", consumer_id, topic);
                    // No creamos un hilo de productor para un consumidor que solo se suscribe
                    continue; // Volver a esperar más conexiones
                } else {
                    fprintf(stderr, "Formato de suscripción incorrecto del cliente %d: '%s'\n", client_fd, buffer);
                    enqueue_log("Formato de suscripción incorrecto del cliente %d: '%s'", client_fd, buffer);
                    close(client_fd);
                    continue;
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