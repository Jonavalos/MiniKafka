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

#define LOG_MSG_SIZE 256
#define LOG_QUEUE_CAPACITY 100
#define SHM_NAME "/log_queue_shm"

// Estructura del mensaje de log con timestamp
typedef struct {
    time_t timestamp;
    char message[LOG_MSG_SIZE];
} LogMessage;

// Cola de logs en memoria compartida
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

// Función para añadir logs a la cola con reintentos
void enqueue_log(const char *msg) {
    if (!log_queue || !msg) return;
    
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
    
    // Copiar mensaje de manera segura
    strncpy(log_queue->messages[log_queue->tail].message, msg, LOG_MSG_SIZE - 1);
    log_queue->messages[log_queue->tail].message[LOG_MSG_SIZE - 1] = '\0';
    
    log_queue->tail = (log_queue->tail + 1) % LOG_QUEUE_CAPACITY;
    log_queue->count++;
    
    // Notificar que hay un nuevo mensaje
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

int main() {
    // Configurar manejadores de señales
    signal(SIGINT, signal_handler);
    signal(SIGTERM, signal_handler);
    
    // Inicializar la cola de logs
    if (init_log_queue() != 0) {
        fprintf(stderr, "Failed to initialize log queue\n");
        return 1;
    }
    
    // Crear thread para procesar logs
    if (pthread_create(&logger_thread, NULL, log_writer_thread, NULL) != 0) {
        perror("Failed to create logger thread");
        cleanup_log_queue();
        return 1;
    }
    
    // Ejemplo de uso del sistema de logs
    enqueue_log("Sistema de logging iniciado");
    enqueue_log("Broker iniciando operaciones");
    
    // Simular procesamiento del broker
    for (int i = 0; i < 5 && running; i++) {
        char buffer[100];
        snprintf(buffer, sizeof(buffer), "Procesando operación %d", i);
        enqueue_log(buffer);
        sleep(1);  // Simular trabajo
    }
    
    enqueue_log("Broker finalizando operaciones");
    
    // Apagar el sistema de logs adecuadamente
    shutdown_logger();
    
    printf("Programa finalizado correctamente\n");
    return 0;
}