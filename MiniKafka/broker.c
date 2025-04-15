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


long long get_last_consumer_offset(const char *topic, const char *group_id, int consumer_id);


// Subscription functions

// Modify add_subscription to check for existing offset
void add_subscription(int consumer_id, int socket_fd, const char *topic, const char *group_id) {
    pthread_mutex_lock(&subscription_mutex);
    pthread_mutex_lock(&group_members_mutex);
    
    // Register the individual subscription
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
            enqueue_log("Consumer %d subscribed to topic '%s' (individual)", consumer_id, topic);
        }
    }

    // Register group membership (if a group_id was provided)
    if (strlen(group_id) > 0 && num_group_members < MAX_GROUP_MEMBERS) {
        group_members[num_group_members].consumer_id = consumer_id;
        group_members[num_group_members].socket_fd = socket_fd;
        strcpy(group_members[num_group_members].topic, topic);
        strcpy(group_members[num_group_members].group_id, group_id);
        num_group_members++;
        
        // Check if this consumer has a previous offset
        long long last_offset = get_last_consumer_offset(topic, group_id, consumer_id);
        if (last_offset >= 0) {
            enqueue_log("Consumer %d rejoined group '%s' for topic '%s', last offset: %lld", 
                       consumer_id, group_id, topic, last_offset);
            
            // Here you could send any missed messages since the last offset
            // This would require storing messages or having a way to retrieve them
            // For now, we'll just acknowledge the last offset
        } else {
            enqueue_log("Consumer %d joined group '%s' for topic '%s' (new consumer)", 
                       consumer_id, group_id, topic);
        }
    }
    
    pthread_mutex_unlock(&group_members_mutex);
    pthread_mutex_unlock(&subscription_mutex);
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



// Función para obtener el siguiente consumidor en un grupo (round-robin)
int get_next_consumer_in_group(const char *topic, const char *group_id) {
    pthread_mutex_lock(&group_members_mutex);
    pthread_mutex_lock(&group_distribution_state_mutex);

    printf("DEBUG: Buscando consumer para topic '%s', grupo '%s'\n", topic, group_id);
    enqueue_log("DEBUG: Buscando consumer para topic '%s', grupo '%s'", topic, group_id);

    int member_count = 0;
    int socket_fds[MAX_GROUP_MEMBERS];
    int consumer_ids[MAX_GROUP_MEMBERS];

    for (int i = 0; i < num_group_members; i++) {
        if (strcmp(group_members[i].topic, topic) == 0 &&
            strcmp(group_members[i].group_id, group_id) == 0 &&
            group_members[i].socket_fd >= 0) {  // Valid socket
            socket_fds[member_count] = group_members[i].socket_fd;
            consumer_ids[member_count] = group_members[i].consumer_id;
            member_count++;
        }
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

void distribute_message(const char *topic, const char *group_id, const char *message) {
    printf("DEBUG: Intentando distribuir mensaje para tópico '%s', grupo '%s'\n", topic, group_id);
    enqueue_log("DEBUG: Intentando distribuir mensaje para tópico '%s', grupo '%s'", topic, group_id);

    int consumer_socket = get_next_consumer_in_group(topic, group_id);
    if (consumer_socket == -1) {
        printf("WARNING: No se pudo encontrar consumidor para el mensaje en el grupo '%s', tópico '%s'\n", group_id, topic);
        enqueue_log("WARNING: No se pudo encontrar consumidor para el mensaje en el grupo '%s', tópico '%s'", group_id, topic);
        return;
    }

    char full_message[MAX_MESSAGE_LENGTH];
    snprintf(full_message, sizeof(full_message), "[%s][%s]: %s", topic, group_id, message);
    ssize_t bytes_sent = send(consumer_socket, full_message, strlen(full_message), 0);
    if (bytes_sent == -1) {
        perror("send");
        printf("ERROR: Falló el envío del mensaje al socket %d\n", consumer_socket);
        enqueue_log("ERROR: Falló el envío del mensaje al socket %d", consumer_socket);
        // Opcional: marcar este socket como inválido en group_members
    } else {
        printf("DEBUG: Mensaje enviado a socket %d: %s\n", consumer_socket, full_message);
        enqueue_log("DEBUG: Mensaje enviado a socket %d: %s", consumer_socket, full_message);
    }
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

// Thread para procesar mensajes y distribuirlos a los consumidores
void *message_processor_thread(void *arg) {
    Message msg;

    while (running || (msg_queue && msg_queue->count > 0)) {
        if (dequeue_message(&msg)) {
            // Log del mensaje dequeued
            printf("DEBUG: Dequeued message ID %lld from producer %d, topic '%s'\n", 
                   msg.id, msg.producer_id, msg.topic);
            enqueue_log("DEBUG: Dequeued message ID %lld from producer %d, topic '%s'", 
                       msg.id, msg.producer_id, msg.topic);

            // Agregar mensaje al historial (opcional)
            pthread_mutex_lock(&message_history_mutex);
            if (message_history_count < MAX_MESSAGE_HISTORY) {
                message_history[message_history_count++] = msg;
            }
            pthread_mutex_unlock(&message_history_mutex);

            // Buscar todos los grupos suscritos a este topic
            pthread_mutex_lock(&group_members_mutex);
            char unique_groups[MAX_GROUP_MEMBERS][64];
            int unique_count = 0;

            for (int i = 0; i < num_group_members; i++) {
                if (strcmp(group_members[i].topic, msg.topic) == 0) {
                    // Ver si ya agregamos este grupo
                    int already_present = 0;
                    for (int j = 0; j < unique_count; j++) {
                        if (strcmp(unique_groups[j], group_members[i].group_id) == 0) {
                            already_present = 1;
                            break;
                        }
                    }

                    if (!already_present && unique_count < MAX_GROUP_MEMBERS) {
                        strcpy(unique_groups[unique_count++], group_members[i].group_id);
                    }
                }
            }
            pthread_mutex_unlock(&group_members_mutex);

            // Enviar mensaje a cada grupo suscrito
            for (int i = 0; i < unique_count; i++) {
                distribute_message(msg.topic, unique_groups[i], msg.payload);
            }
        }

        // Evitar uso excesivo de CPU
        usleep(1000); // 1 ms
    }

    return NULL;
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

// bool dequeue_message(Message *out_msg) {
//     if (!msg_queue || !out_msg) return false;

//     pthread_mutex_lock(&msg_queue->mutex);

//     while (msg_queue->count == 0 && !msg_queue->shutdown) {
//         pthread_cond_wait(&msg_queue->not_empty, &msg_queue->mutex);
//     }

//     if (msg_queue->count == 0 && msg_queue->shutdown) {
//         pthread_mutex_unlock(&msg_queue->mutex);
//         return false;
//     }

//     *out_msg = msg_queue->messages[msg_queue->head];
//     msg_queue->head = (msg_queue->head + 1) % MSG_QUEUE_CAPACITY;
//     msg_queue->count--;

//     pthread_cond_signal(&msg_queue->not_full);
//     pthread_mutex_unlock(&msg_queue->mutex);
//     return true;
// }

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
                sprintf(confirm_msg, "SUBSCRIBED:%d:%s:%s", consumer_id, topic, group_id);
                write(client_fd, confirm_msg, strlen(confirm_msg));
            } else {
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

// Store the offset for a consumer group
void update_consumer_offset(const char *topic, const char *group_id, int consumer_id, long long message_id) {
    pthread_mutex_lock(&consumer_offsets_mutex);
    
    // Look for an existing offset entry
    int offset_idx = -1;
    for (int i = 0; i < num_consumer_offsets; i++) {
        if (strcmp(consumer_offsets[i].topic, topic) == 0 && 
            strcmp(consumer_offsets[i].group_id, group_id) == 0 &&
            consumer_offsets[i].consumer_id == consumer_id) {
            offset_idx = i;
            break;
        }
    }
    
    // If not found, create a new entry
    if (offset_idx == -1) {
        if (num_consumer_offsets < MAX_CONSUMER_OFFSETS) {
            offset_idx = num_consumer_offsets++;
            strcpy(consumer_offsets[offset_idx].topic, topic);
            strcpy(consumer_offsets[offset_idx].group_id, group_id);
            consumer_offsets[offset_idx].consumer_id = consumer_id;
        } else {
            enqueue_log("ERROR: Maximum number of consumer offsets reached");
            pthread_mutex_unlock(&consumer_offsets_mutex);
            return;
        }
    }
    
    // Update the offset
    consumer_offsets[offset_idx].last_consumed_offset = message_id;
    enqueue_log("Updated offset for group '%s', topic '%s', consumer %d to %lld", 
                group_id, topic, consumer_id, message_id);
    
    pthread_mutex_unlock(&consumer_offsets_mutex);
}

// Get the last consumed offset for a consumer group
long long get_last_consumer_offset(const char *topic, const char *group_id, int consumer_id) {
    long long offset = -1; // -1 means no previous offset (start from beginning)
    
    pthread_mutex_lock(&consumer_offsets_mutex);
    
    for (int i = 0; i < num_consumer_offsets; i++) {
        if (strcmp(consumer_offsets[i].topic, topic) == 0 && 
            strcmp(consumer_offsets[i].group_id, group_id) == 0 &&
            consumer_offsets[i].consumer_id == consumer_id) {
            offset = consumer_offsets[i].last_consumed_offset;
            break;
        }
    }
    
    pthread_mutex_unlock(&consumer_offsets_mutex);
    
    return offset;
}

// Get the last consumed offset for an entire consumer group
long long get_group_last_offset(const char *topic, const char *group_id) {
    long long max_offset = -1;
    
    pthread_mutex_lock(&consumer_offsets_mutex);
    
    for (int i = 0; i < num_consumer_offsets; i++) {
        if (strcmp(consumer_offsets[i].topic, topic) == 0 && 
            strcmp(consumer_offsets[i].group_id, group_id) == 0) {
            if (consumer_offsets[i].last_consumed_offset > max_offset) {
                max_offset = consumer_offsets[i].last_consumed_offset;
            }
        }
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
    
    for (int i = 0; i < num_consumer_offsets; i++) {
        fprintf(offset_file, "%s,%s,%d,%lld\n", 
                consumer_offsets[i].topic,
                consumer_offsets[i].group_id,
                consumer_offsets[i].consumer_id,
                consumer_offsets[i].last_consumed_offset);
    }
    
    pthread_mutex_unlock(&consumer_offsets_mutex);
    
    fclose(offset_file);
    enqueue_log("Consumer offsets saved to file");
}

// Function to load consumer offsets on startup
void load_consumer_offsets() {
    FILE *offset_file = fopen("consumer_offsets.dat", "r");
    if (!offset_file) {
        // File might not exist yet, which is fine
        if (errno != ENOENT) {
            enqueue_log("WARNING: Failed to open consumer_offsets.dat for reading: %s", 
                       strerror(errno));
        }
        return;
    }
    
    pthread_mutex_lock(&consumer_offsets_mutex);
    num_consumer_offsets = 0;
    
    char line[256];
    while (fgets(line, sizeof(line), offset_file) && num_consumer_offsets < MAX_CONSUMER_OFFSETS) {
        char topic[64];
        char group_id[64];
        int consumer_id;
        long long offset;
        
        if (sscanf(line, "%63[^,],%63[^,],%d,%lld", 
                   topic, group_id, &consumer_id, &offset) == 4) {
            
            strcpy(consumer_offsets[num_consumer_offsets].topic, topic);
            strcpy(consumer_offsets[num_consumer_offsets].group_id, group_id);
            consumer_offsets[num_consumer_offsets].consumer_id = consumer_id;
            consumer_offsets[num_consumer_offsets].last_consumed_offset = offset;
            num_consumer_offsets++;
        }
    }
    
    pthread_mutex_unlock(&consumer_offsets_mutex);
    
    fclose(offset_file);
    enqueue_log("Loaded %d consumer offsets from file", num_consumer_offsets);
}

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

// Function to send messages from a specific offset
void send_messages_from_offset(int consumer_id, int client_fd, const char *topic, 
                               const char *group_id, long long start_offset) {
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