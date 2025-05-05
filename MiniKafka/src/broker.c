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
#include <semaphore.h>

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
    sem_t empty_slots; // Semáforo para espacios vacíos
    sem_t filled_slots; // Semáforo para mensajes llenos
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

typedef struct {
    int fd;
    int producer_id;
} ProdArg;

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

// Subscription functions
void add_subscription(int consumer_id, int socket_fd, const char *topic, const char *group_id);

void distribute_message(const char *topic, const char *message);

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
void *producer_handler_thread_fd(void *arg);
int get_next_consumer_in_group(const char *topic, const char *group_id);
void save_consumer_offsets();
void send_messages_from_offset(int consumer_id, int client_fd, const char *topic, const char *group_id, long long start_offset);
void store_message_in_history(const Message *msg);
long long get_last_consumer_offset(const char *topic, const char *group_id, int consumer_id);


void remove_subscription(int consumer_id, const char *topic);
void remove_group_distribution_state(const char *topic, const char *group_id);
long long get_group_last_offset(const char *topic, const char *group_id);
void update_consumer_offset(const char *topic, const char *group_id, int consumer_id, long long message_id);
void process_subscription_request(int consumer_id, int socket_fd, const char *topic, const char *group_id, long long start_offset);


typedef struct GroupMember {
    char topic[64];
    char group_id[64];
    int consumer_id;
    int socket_fd;
    struct GroupMember *next; // Puntero al siguiente nodo
} GroupMember;

GroupMember *group_members = NULL; // Lista enlazada de miembros de grupo
pthread_mutex_t group_members_mutex = PTHREAD_MUTEX_INITIALIZER;



//******************** T H R E A D   P O O L******************* */
#include <stdlib.h>
#include <stdbool.h>
#include <pthread.h>
#include <stdio.h>
#include <unistd.h>
#include <sys/socket.h>
#include <poll.h>
#include <errno.h>

#define THREAD_POOL_SIZE 50
#define MAX_TOPIC_LEN 64

// Tipos de tarea posibles
typedef enum {
    TASK_SUBSCRIBE,
    TASK_UNSUBSCRIBE,
    TASK_PRODUCE
} TaskType;

// Datos de la tarea
typedef struct {
    TaskType   type;
    int        client_fd;
    int        consumer_id;
    char       topic[MAX_TOPIC_LEN];
    char       group_id[MAX_TOPIC_LEN];
    long long  start_offset;
} TaskData;

// Nodo de la cola enlazada
typedef struct TaskNode {
    TaskData       data;
    struct TaskNode *next;
} TaskNode;

// Cola de tareas
typedef struct {
    TaskNode       *head;
    TaskNode       *tail;
    pthread_mutex_t mutex;
    pthread_cond_t  not_empty;
    bool             shutdown;
} TaskQueue;

// Pool de hilos
typedef struct {
    pthread_t threads[THREAD_POOL_SIZE];
    TaskQueue queue;
} ThreadPool;

ThreadPool thread_pool;  // Definido en broker.c

// Declara prototipos de funciones externas a usar en worker
// extern void add_subscription(int consumer_id, int client_fd, const char *topic, const char *group_id);
// extern void remove_subscription(int consumer_id, const char *topic);
// extern void send_messages_from_offset(int consumer_id, int client_fd, const char *topic, const char *group_id, long long offset);
// extern void producer_handler_thread(void *arg);

// Inicializa la cola de tareas
void init_task_queue(TaskQueue *q) {
    q->head = q->tail = NULL;
    q->shutdown = false;
    pthread_mutex_init(&q->mutex, NULL);
    pthread_cond_init(&q->not_empty, NULL);
}

// Encola una tarea
void enqueue_task_data(TaskQueue *q, TaskData *td) {
    TaskNode *node = malloc(sizeof(*node));
    if (!node) {
        perror("malloc TaskNode");
        return;
    }
    node->data = *td;
    node->next = NULL;

    pthread_mutex_lock(&q->mutex);
    if (q->tail) q->tail->next = node;
    else        q->head = node;
    q->tail = node;
    pthread_cond_signal(&q->not_empty);
    pthread_mutex_unlock(&q->mutex);
}

// Desencola una tarea o NULL si shutdown
TaskNode *dequeue_task(TaskQueue *q) {
    pthread_mutex_lock(&q->mutex);
    while (!q->head && !q->shutdown) {
        pthread_cond_wait(&q->not_empty, &q->mutex);
    }
    if (q->shutdown && !q->head) {
        pthread_mutex_unlock(&q->mutex);
        return NULL;
    }
    TaskNode *node = q->head;
    q->head = node->next;
    if (!q->head) q->tail = NULL;
    pthread_mutex_unlock(&q->mutex);
    return node;
}

// Loop de trabajo del worker
void *worker_loop(void *arg) {
    TaskQueue *q = arg;
    while (true) {
        TaskNode *node = dequeue_task(q);
        if (!node) break;
        TaskData *t = &node->data;
        switch (t->type) {
        case TASK_SUBSCRIBE:
            // Llama a process_subscription_request en lugar de add_subscription y send_messages_from_offset
            process_subscription_request(t->consumer_id, t->client_fd, t->topic, t->group_id, t->start_offset);
            break;
        case TASK_UNSUBSCRIBE:
            remove_subscription(t->consumer_id, t->topic);
            close(t->client_fd);
            break;
        case TASK_PRODUCE:
            // En el nuevo diseño, el handler de productores se lanza directamente
            // desde connection_handler_thread y no se encola aquí.
            break;
        }
        free(node);
    }
    return NULL;
}

// Inicializa el pool
void init_thread_pool(ThreadPool *p) {
    init_task_queue(&p->queue);
    for (int i = 0; i < THREAD_POOL_SIZE; i++) {
        pthread_create(&p->threads[i], NULL, worker_loop, &p->queue);
    }
}

// Apaga el pool limpiamente
void shutdown_thread_pool(ThreadPool *p) {
    pthread_mutex_lock(&p->queue.mutex);
    p->queue.shutdown = true;
    pthread_cond_broadcast(&p->queue.not_empty);
    pthread_mutex_unlock(&p->queue.mutex);

    for (int i = 0; i < THREAD_POOL_SIZE; i++) {
        pthread_join(p->threads[i], NULL);
    }
    pthread_mutex_destroy(&p->queue.mutex);
    pthread_cond_destroy(&p->queue.not_empty);
}


  //************************ F I N    T H R E A D    P O O L ************** */



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


// Modified remove_group_member function to check if this was the last member
void remove_group_member(const char *topic, const char *group_id, int consumer_id) {
    pthread_mutex_lock(&group_members_mutex);

    GroupMember *current = group_members;
    GroupMember *prev = NULL;
    int is_found = 0;
    
    // First pass: find and remove the specific member
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
            is_found = 1;
            enqueue_log("Removed group member: topic='%s', group='%s', consumer_id=%d",
                       topic, group_id, consumer_id);
            break;
        }
        prev = current;
        current = current->next;
    }
    
    if (is_found) {
        // Second pass: check if any members of this group/topic still exist
        int members_left = 0;
        current = group_members;
        
        while (current != NULL) {
            if (strcmp(current->topic, topic) == 0 &&
                strcmp(current->group_id, group_id) == 0) {
                members_left++;
                break;
            }
            current = current->next;
        }
        
        // If no members left, clean up the distribution state
        if (members_left == 0) {
            enqueue_log("Last member removed from topic='%s', group='%s', cleaning up group state",
                       topic, group_id);
            pthread_mutex_unlock(&group_members_mutex);
            remove_group_distribution_state(topic, group_id);
            pthread_mutex_lock(&group_members_mutex);
        }
    }

    pthread_mutex_unlock(&group_members_mutex);
}


// Function to remove a distribution state when a group becomes empty
void remove_group_distribution_state(const char *topic, const char *group_id) {
    pthread_mutex_lock(&group_distribution_state_mutex);
    
    int found_index = -1;
    for (int i = 0; i < num_group_distribution_states; i++) {
        if (strcmp(group_distribution_states[i].topic, topic) == 0 &&
            strcmp(group_distribution_states[i].group_id, group_id) == 0) {
            found_index = i;
            break;
        }
    }
    
    if (found_index != -1) {
        // Move the last entry to the position of the removed entry
        if (found_index < num_group_distribution_states - 1) {
            memcpy(&group_distribution_states[found_index], 
                   &group_distribution_states[num_group_distribution_states - 1],
                   sizeof(GroupDistributionState));
        }
        num_group_distribution_states--;
        enqueue_log("Removed distribution state for topic '%s', group '%s'", topic, group_id);
    }
    
    pthread_mutex_unlock(&group_distribution_state_mutex);
}

// This is the function you should call when a consumer disconnects
// Función completa de manejar desconexión de consumidor
void handle_consumer_disconnect(int consumer_id) {
    pthread_mutex_lock(&group_members_mutex);
    
    // Almacenar todos los topic/group de este consumidor
    typedef struct {
        char topic[64];
        char group_id[64];
    } TopicGroup;
    
    TopicGroup to_remove[100]; // Ajustar según sea necesario
    int remove_count = 0;
    
    GroupMember *current = group_members;
    while (current != NULL && remove_count < 100) {
        if (current->consumer_id == consumer_id) {
            // Copiar en lugar de usar punteros
            strncpy(to_remove[remove_count].topic, current->topic, sizeof(to_remove[0].topic) - 1);
            to_remove[remove_count].topic[sizeof(to_remove[0].topic) - 1] = '\0';
            
            strncpy(to_remove[remove_count].group_id, current->group_id, sizeof(to_remove[0].group_id) - 1);
            to_remove[remove_count].group_id[sizeof(to_remove[0].group_id) - 1] = '\0';
            
            remove_count++;
        }
        current = current->next;
    }
    
    pthread_mutex_unlock(&group_members_mutex);
    
    // Ahora eliminar este consumidor de todos sus grupos
    for (int i = 0; i < remove_count; i++) {
        enqueue_log("DEBUG: Desconexión: eliminando consumidor %d del tópico '%s', grupo '%s'", 
                   consumer_id, to_remove[i].topic, to_remove[i].group_id);
        remove_group_member(to_remove[i].topic, to_remove[i].group_id, consumer_id);
        remove_subscription(consumer_id, to_remove[i].topic);
    }
    
    enqueue_log("INFO: Consumidor %d desconectado completamente de %d grupos/tópicos", 
               consumer_id, remove_count);
}

// Esta función debe ser llamada cuando se detecta una desconexión de socket
void handle_socket_disconnect(int socket_fd) {
    pthread_mutex_lock(&group_members_mutex);
    
    int consumer_id = -1;
    GroupMember *current = group_members;
    
    // Buscar el consumer_id correspondiente a este socket
    while (current != NULL) {
        if (current->socket_fd == socket_fd) {
            consumer_id = current->consumer_id;
            break;
        }
        current = current->next;
    }
    
    pthread_mutex_unlock(&group_members_mutex);
    
    if (consumer_id != -1) {
        enqueue_log("DEBUG: Socket %d corresponde al consumidor %d, manejando desconexión", 
                   socket_fd, consumer_id);
        handle_consumer_disconnect(consumer_id);
    } else {
        enqueue_log("WARNING: No se encontró consumidor para el socket %d", socket_fd);
    }
    
    // Asegurarse de que el socket esté cerrado
    close(socket_fd);
}


//************************* F I N    G R O U P    M E M B E R S***************** */


//************************* S U B S C R I P T I O N S ***************** */

void process_subscription_request(int consumer_id, int socket_fd, const char *topic, const char *group_id, long long start_offset) {
    if (start_offset == -1) {
        // Actualizar el offset al último mensaje disponible
        long long last_offset = get_group_last_offset(topic, group_id);
        update_consumer_offset(topic, group_id, consumer_id, last_offset + 1);
    } else {
        // Usar el offset proporcionado
        update_consumer_offset(topic, group_id, consumer_id, start_offset);
    }

    // Registrar al consumidor en el grupo
    add_group_member(topic, group_id, consumer_id, socket_fd);
}


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
    } else {
        // Si ya existe, actualiza el socket y limpia los topics
        current->socket_fd = socket_fd;
        current->topic_count = 0;
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

    // Actualizar el offset del consumidor al último mensaje disponible
    long long last_offset = get_group_last_offset(topic, group_id);
    update_consumer_offset(topic, group_id, consumer_id, last_offset + 1);

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
        enqueue_log("WARNING: No hay miembros en el grupo '%s' para el tópico '%s', limpiando estado", 
                    group_id, topic);
        printf("WARNING: No hay miembros en el grupo '%s' para el tópico '%s', limpiando estado\n", 
               group_id, topic);
        // Clean up the state since we have no members
        pthread_mutex_unlock(&group_distribution_state_mutex);
        pthread_mutex_unlock(&group_members_mutex);
        remove_group_distribution_state(topic, group_id);
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
            enqueue_log("DEBUG: Estado de distribución creado para topic '%s', grupo '%s'", topic, group_id);
            printf("DEBUG: Estado de distribución creado para topic '%s', grupo '%s'\n", topic, group_id);
        } else {
            enqueue_log("ERROR: Límite de estados de distribución alcanzado");
            printf("ERROR: Límite de estados de distribución alcanzado\n");
            pthread_mutex_unlock(&group_distribution_state_mutex);
            pthread_mutex_unlock(&group_members_mutex);
            return -1;
        }
    }

    int current_index = group_distribution_states[state_index].next_consumer_index;


    // Check if we have any group distribution state for this topic/group
    int state_exists = 0;
    for (int i = 0; i < num_group_distribution_states; i++) {
        if (strcmp(group_distribution_states[i].topic, topic) == 0 &&
            strcmp(group_distribution_states[i].group_id, group_id) == 0) {
            state_exists = 1;
            break;
        }
    }
    
    // If no state exists, it means the group is empty or has been cleaned up
    if (!state_exists) {//AQUI
        enqueue_log("No distribution state for topic '%s', grupo '%s', skipping", topic, group_id);
        printf("No distribution state for topic '%s', grupo '%s', skipping\n", topic, group_id);
        pthread_mutex_unlock(&group_distribution_state_mutex);
        pthread_mutex_unlock(&group_members_mutex);
        return -1;
    }



    // Validación del índice
    if (current_index >= member_count || current_index < 0) {
        enqueue_log("WARNING: Índice fuera de rango (%d), reiniciando", current_index);
        printf("WARNING: Índice fuera de rango (%d), reiniciando\n", current_index);
        current_index = 0;
        group_distribution_states[state_index].next_consumer_index = 1 % member_count;
    } else {
        group_distribution_states[state_index].next_consumer_index = (current_index + 1) % member_count;
    }

    int selected_socket = socket_fds[current_index];
    int selected_consumer = consumer_ids[current_index];

    enqueue_log("DEBUG: Seleccionado consumidor %d con socket %d (índice %d de %d)",
                selected_consumer, selected_socket, current_index, member_count);
    printf("DEBUG: Seleccionado consumidor %d con socket %d (índice %d de %d)\n",
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
            distribute_message(msg.topic, msg.payload);
        }

        // Evitar uso excesivo de CPU
        usleep(1000); // 1 ms
    }

    return NULL;
}

// Versión corregida de distribute_message
void distribute_message(const char *topic, const char *message) {
    printf("DEBUG: Intentando distribuir mensaje para tópico '%s'\n", topic);
    enqueue_log("DEBUG: Intentando distribuir mensaje para tópico '%s'", topic);
    
    // 1) Primero recolectamos los group_id únicos en una lista auxiliar
    #define MAX_GROUPS 100
    char seen_groups[MAX_GROUPS][64]; // Almacenamos strings completos, no punteros
    int  seen_count = 0;
    
    pthread_mutex_lock(&group_members_mutex);
    for (GroupMember *cur = group_members; cur; cur = cur->next) {
        if (strcmp(cur->topic, topic) != 0) continue;
        
        // ¿Ya vimos este group_id?
        bool found = false;
        for (int i = 0; i < seen_count; i++) {
            if (strcmp(seen_groups[i], cur->group_id) == 0) {
                found = true;
                break;
            }
        }
        
        if (!found && seen_count < MAX_GROUPS) {
            // Hacer una copia completa del string, no solo el puntero
            strncpy(seen_groups[seen_count], cur->group_id, sizeof(seen_groups[0]) - 1);
            seen_groups[seen_count][sizeof(seen_groups[0]) - 1] = '\0'; // Asegurar terminación
            seen_count++;
        }
    }
    pthread_mutex_unlock(&group_members_mutex);
    
    enqueue_log("DEBUG: Encontrados %d grupos para tópico '%s'", seen_count, topic);
    printf("DEBUG: Encontrados %d grupos para tópico '%s'\n", seen_count, topic);   
    
    // 2) Para cada grupo único, hacemos el round-robin de envio
    for (int gi = 0; gi < seen_count; gi++) {
        const char *group_id = seen_groups[gi];
        enqueue_log("DEBUG: Procesando grupo '%s' para tópico '%s'", group_id, topic);
        printf("DEBUG: Procesando grupo '%s' para tópico '%s'\n", group_id, topic);
        
        char full_message[MAX_MESSAGE_LENGTH];
        snprintf(full_message, sizeof(full_message), "%s", message);
        
        // Verificar si el grupo tiene miembros antes de intentar enviar
        pthread_mutex_lock(&group_members_mutex);
        bool has_members = false;
        for (GroupMember *cur = group_members; cur; cur = cur->next) {
            if (strcmp(cur->topic, topic) == 0 && strcmp(cur->group_id, group_id) == 0) {
                has_members = true;
                break;
            }
        }
        pthread_mutex_unlock(&group_members_mutex);
        
        if (!has_members) {
            enqueue_log("DEBUG: El grupo '%s' no tiene miembros activos, saltando", group_id);
            printf("DEBUG: El grupo '%s' no tiene miembros activos, saltando\n", group_id);
            continue;
        }
        
        // Igual que tu versión que funcionaba: loop hasta enviar o agotar consumidores
        int retry_count = 0;
        const int MAX_RETRIES = 3; // Límite para evitar bucles infinitos
        
        while (retry_count < MAX_RETRIES) {
            int consumer_socket = get_next_consumer_in_group(topic, group_id);
            
            if (consumer_socket < 0) { //AQUI
                enqueue_log("WARNING: No hay consumidores disponibles para tópico '%s', grupo '%s'; descartando mensaje",
                           topic, group_id);
                printf("WARNING: No hay consumidores disponibles para tópico '%s', grupo '%s'; descartando mensaje\n",
                       topic, group_id);
                break;
            }
            
            // Detectar hang-up
            struct pollfd pfd = { .fd = consumer_socket, .events = POLLIN|POLLHUP };
            if (poll(&pfd, 1, 0) > 0 && (pfd.revents & POLLHUP)) {
                int cid = find_consumer_id_by_socket(consumer_socket, topic, group_id);
                if (cid >= 0) {
                    enqueue_log("DEBUG: Consumidor %d colgado; removiendo", cid);
                    printf("DEBUG: Consumidor %d colgado; removiendo\n", cid);
                    remove_group_member(topic, group_id, cid);
                    remove_subscription(cid, topic);
                }
                // No cerrar el socket aquí si ya fue cerrado por el cliente
                retry_count++;
                continue;
            }
            
            // Intentar envío con un mensaje completo y terminado correctamente
            full_message[sizeof(full_message) - 1] = '\0'; // Asegurar terminación
            ssize_t message_len = strlen(full_message);
            ssize_t w = send(consumer_socket, full_message, message_len, MSG_NOSIGNAL);
            
            if (w == message_len) { // Verificar que se envió el mensaje completo
                enqueue_log("DEBUG: Mensaje enviado con éxito a socket %d en grupo '%s'", 
                           consumer_socket, group_id);
                printf("DEBUG: Mensaje enviado con éxito a socket %d en grupo '%s'\n",
                       consumer_socket, group_id); 
                break; // Éxito - salir del bucle
            } else {
                int cid = find_consumer_id_by_socket(consumer_socket, topic, group_id);
                if (cid >= 0) {
                    enqueue_log("ERROR: Falló envío a consumidor %d; removiendo (send retornó %zd, esperado %zd)", 
                               cid, w, message_len);
                    printf("ERROR: Falló envío a consumidor %d; removiendo (send retornó %zd, esperado %zd)\n",
                           cid, w, message_len);
                    remove_group_member(topic, group_id, cid);
                    remove_subscription(cid, topic);
                }
                
                if (w == -1 && (errno == EPIPE || errno == ECONNRESET)) {
                    // El socket está cerrado por el otro extremo
                    close(consumer_socket);
                }
                
                retry_count++;
                continue;
            }
        }
        
        if (retry_count >= MAX_RETRIES) {
            enqueue_log("ERROR: Agotado número máximo de intentos (%d) para tópico '%s', grupo '%s'", 
                       MAX_RETRIES, topic, group_id);
            printf("ERROR: Agotado número máximo de intentos (%d) para tópico '%s', grupo '%s'\n",
                   MAX_RETRIES, topic, group_id);
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
    pthread_mutexattr_init(&mattr);
    pthread_mutexattr_setpshared(&mattr, PTHREAD_PROCESS_SHARED);

    pthread_mutex_init(&msg_queue->mutex, &mattr);
    sem_init(&msg_queue->empty_slots, 1, MSG_QUEUE_CAPACITY); // Inicializa con capacidad máxima
    sem_init(&msg_queue->filled_slots, 1, 0); // Inicializa con 0 mensajes llenos

    msg_queue->head = 0;
    msg_queue->tail = 0;
    msg_queue->count = 0;
    msg_queue->shutdown = false;
    msg_queue->next_message_id = 0;

    pthread_mutexattr_destroy(&mattr);
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

    sem_wait(&msg_queue->empty_slots); // Espera a que haya espacio disponible
    pthread_mutex_lock(&msg_queue->mutex);

    Message *msg = &msg_queue->messages[msg_queue->tail];
    msg->id = msg_queue->next_message_id++;
    msg->producer_id = producer_id;
    strncpy(msg->topic, topic, sizeof(msg->topic) - 1);
    strncpy(msg->payload, payload, sizeof(msg->payload) - 1);
    msg->topic[sizeof(msg->topic) - 1] = '\0';
    msg->payload[sizeof(msg->payload) - 1] = '\0';

    msg_queue->tail = (msg_queue->tail + 1) % MSG_QUEUE_CAPACITY;
    msg_queue->count++;

    pthread_mutex_unlock(&msg_queue->mutex);
    sem_post(&msg_queue->filled_slots); // Incrementa el contador de mensajes llenos
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
// Modify the connection handler to use the thread pool
// connection_handler_thread.c
// Maneja conexiones entrantes: consumidores via thread pool, productores con hilo dedicado

// connection_handler_thread.c
// Maneja conexiones entrantes: consumidores via thread pool, productores con hilo dedicado

// Declaración del helper para leer N bytes
static ssize_t read_n_bytes(int fd, void *buf, size_t count);

void *connection_handler_thread(void *arg) {
    int server_fd = *((int *)arg);
    struct sockaddr_in client_addr;
    socklen_t client_len = sizeof(client_addr);
    int client_fd;
    char peek_buf[16];

    printf("Waiting for connections...\n");
    while (running) {
        client_fd = accept(server_fd, (struct sockaddr *)&client_addr, &client_len);
        if (client_fd < 0) {
            if (errno == EINTR) continue;
            if (errno == EAGAIN || errno == EWOULDBLOCK) continue;
            perror("accept failed");
            break;
        }
        printf("Client connected with FD: %d\n", client_fd);

        // Peeking de primeros bytes
        ssize_t got = recv(client_fd, peek_buf, sizeof(peek_buf), MSG_PEEK);
        if (got <= 0) {
            close(client_fd);
            continue;
        }
        peek_buf[(got < sizeof(peek_buf) ? got : sizeof(peek_buf)-1)] = '\0';

        if (strncmp(peek_buf, "SUBSCRIBE:", 10) == 0 || strncmp(peek_buf, "UNSUBSCRIBE:", 12) == 0) {
            // Consumer: leemos la línea completa
            char linebuf[256];
            ssize_t n = recv(client_fd, linebuf, sizeof(linebuf)-1, 0);
            if (n <= 0) { close(client_fd); continue; }
            linebuf[n] = '\0';

            TaskData *td = malloc(sizeof(*td));
            if (!td) { perror("malloc TaskData"); close(client_fd); continue; }
            memset(td, 0, sizeof(*td));
            td->client_fd = client_fd;

            if (strncmp(linebuf, "SUBSCRIBE:", 10) == 0) {
                td->type = TASK_SUBSCRIBE;
                sscanf(linebuf, "SUBSCRIBE:%d:%63[^:]:%63[^:]:%lld",
                       &td->consumer_id, td->topic, td->group_id, &td->start_offset);
            } else {
                td->type = TASK_UNSUBSCRIBE;
                sscanf(linebuf, "UNSUBSCRIBE:%d:%63s", &td->consumer_id, td->topic);
            }
            enqueue_task_data(&thread_pool.queue, td);
        } else {
            // Producer: consumimos handshake binario
            int client_type;
            int producer_id;
            if (read_n_bytes(client_fd, &client_type, sizeof(client_type)) != sizeof(client_type) ||
                read_n_bytes(client_fd, &producer_id, sizeof(producer_id)) != sizeof(producer_id)) {
                perror("Handshake failed");
                close(client_fd);
                continue;
            }

            // Creamos argumento para el hilo de producer
            typedef struct {
                int fd;
                int producer_id;
            } ProdArg;

            ProdArg *parg = malloc(sizeof(*parg));
            if (!parg) { perror("malloc ProdArg"); close(client_fd); continue; }
            parg->fd = client_fd;
            parg->producer_id = producer_id;

            pthread_t tid;
            pthread_create(&tid, NULL, producer_handler_thread_fd, parg);
            pthread_detach(tid);
        }
    }
    return NULL;
}

// Implementación del helper read_n_bytes
static ssize_t read_n_bytes(int fd, void *buf, size_t count) {
    size_t total = 0;
    while (total < count) {
        ssize_t n = read(fd, (char*)buf + total, count - total);
        if (n <= 0) return n;
        total += n;
    }
    return (ssize_t)total;
}


// Función para manejar un productor conectado
//se cambio de void* a void * para poder usar el thread pool
void *producer_handler_thread_fd(void *arg) {
    // 1. Recuperar los datos
    ProdArg *p = (ProdArg *)arg;
    int client_fd    = p->fd;
    int producer_id  = p->producer_id;
    free(p);

    Message msg;
    while (running) {
        // Leer handshake de mensajes, control total_bytes como antes
        size_t total = 0;
        while (total < sizeof(msg)) {
            ssize_t bytes = read(client_fd,
                                 ((char *)&msg) + total,
                                 sizeof(msg) - total);
            if (bytes <= 0) {
                if (bytes == 0)
                    printf("DEBUG: productor %d desconectado (fd=%d)\n",
                           producer_id, client_fd);
                else
                    perror("ERROR: Fallo en read");
                close(client_fd);
                return NULL;
            }
            total += bytes;
        }

        // Validar y procesar
        if (strlen(msg.topic) == 0 || strlen(msg.payload) == 0) {
            fprintf(stderr,
                    "ERROR: Mensaje corrupto de producer %d (fd=%d)\n",
                    producer_id, client_fd);
            close(client_fd);
            return NULL;
        }

        printf(">> Mensaje recibido de producer %d: topic='%s' payload='%s'\n",
               producer_id, msg.topic, msg.payload);

        enqueue_message(producer_id, msg.topic, msg.payload);

        // Listo para el siguiente
    }

    // Si sales del bucle
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

    // Implementación de un buffer circular
    if (message_history_count < MAX_MESSAGE_HISTORY) {
        message_history[message_history_count++] = *msg;
    } else {
        // Desplazar los mensajes para hacer espacio
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

    sem_wait(&msg_queue->filled_slots); // Espera a que haya mensajes disponibles
    pthread_mutex_lock(&msg_queue->mutex);

    *out_msg = msg_queue->messages[msg_queue->head];
    msg_queue->head = (msg_queue->head + 1) % MSG_QUEUE_CAPACITY;
    msg_queue->count--;

    pthread_mutex_unlock(&msg_queue->mutex);
    sem_post(&msg_queue->empty_slots); // Incrementa el contador de espacios vacíos

    // Almacenar el mensaje en el historial
    store_message_in_history(out_msg);

    return true;
}



int main() {
    // Variables para hilos
    pthread_t logger_thread;
    pthread_t msg_processor_thread;
    pthread_t acceptor_thread;

    // 1) Manejo de señales para shutdown limpio
    signal(SIGINT, signal_handler);
    signal(SIGTERM, signal_handler);

    // 2) Inicializar cola de logs
    if (init_log_queue() != 0) {
        fprintf(stderr, "Failed to initialize log queue\n");
        return 1;
    }
    enqueue_log("Broker iniciado y escuchando en el puerto 8080");

    // 3) Inicializar cola de mensajes
    if (init_msg_queue() != 0) {
        fprintf(stderr, "Failed to initialize message queue\n");
        cleanup_log_queue();
        return 1;
    }

    // 4) Cargar offsets previos (si los usas)
    load_consumer_offsets();

    // 5) Inicializar thread pool (workers para SUBSCRIBE/UNSUBSCRIBE/PRODUCE)
    init_thread_pool(&thread_pool);

    // 6) Crear hilo de logger
    if (pthread_create(&logger_thread, NULL, log_writer_thread, NULL) != 0) {
        perror("Failed to create logger thread");
        shutdown_thread_pool(&thread_pool);
        cleanup_msg_queue();
        cleanup_log_queue();
        return 1;
    }

    // 7) Crear hilo de procesamiento de mensajes
    if (pthread_create(&msg_processor_thread, NULL,
                       message_processor_thread, NULL) != 0) {
        perror("Failed to create message processor thread");
        // señalizar pool y logger para shutdown
        shutdown_thread_pool(&thread_pool);
        shutdown_logger();
        cleanup_msg_queue();
        cleanup_log_queue();
        return 1;
    }

    // 8) Configurar socket del broker
    int server_fd;
    struct sockaddr_in address;
    int opt = 1;
    int port = 8080;

    server_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (server_fd < 0) {
        perror("socket failed");
        goto CLEANUP_ALL;
    }

    setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR | SO_REUSEPORT,
               &opt, sizeof(opt));

    address.sin_family = AF_INET;
    address.sin_addr.s_addr = INADDR_ANY;
    address.sin_port = htons(port);

    if (bind(server_fd, (struct sockaddr *)&address,
             sizeof(address)) < 0) {
        perror("bind failed");
        close(server_fd);
        goto CLEANUP_ALL;
    }

    if (listen(server_fd, SOMAXCONN) < 0) {
        perror("listen failed");
        close(server_fd);
        goto CLEANUP_ALL;
    }

    // 9) Lanzar hilo único de accept
    if (pthread_create(&acceptor_thread, NULL,
                       connection_handler_thread, &server_fd) != 0) {
        perror("Failed to create connection handler thread");
        close(server_fd);
        goto CLEANUP_ALL;
    }

    // 10) Esperar a señal de shutdown
    while (running) {
        sleep(1);
    }

    // 11) Iniciar apagado ordenado
    enqueue_log("Iniciando apagado del broker");

    // 11a) Cerrar el socket de accept
    close(server_fd);

    // 11b) Shutdown thread pool
    shutdown_thread_pool(&thread_pool);

    // 11c) Shutdown logger
    shutdown_logger();

    // 11d) Señalizar shutdown a message_processor
    pthread_mutex_lock(&msg_queue->mutex);
    msg_queue->shutdown = true;
    pthread_cond_signal(&msg_queue->not_empty);
    pthread_mutex_unlock(&msg_queue->mutex);

    // 11e) Esperar a threads
    pthread_cancel(acceptor_thread);
    pthread_join(acceptor_thread, NULL);
    pthread_join(msg_processor_thread, NULL);
    pthread_join(logger_thread, NULL);

    // 11f) Limpiar colas
    cleanup_msg_queue();
    cleanup_log_queue();

    return 0;

CLEANUP_ALL:
    // Si falló antes de iniciar los threads
    shutdown_thread_pool(&thread_pool);
    shutdown_logger();
    cleanup_msg_queue();
    cleanup_log_queue();
    return 1;
}