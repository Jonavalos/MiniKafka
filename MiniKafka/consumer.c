#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <signal.h>
#include <errno.h>
#include <pthread.h>

#define MSG_PAYLOAD_SIZE 256

// Message structure (debe coincidir con la definición en el broker)
typedef struct {
    int producer_id;
    char topic[64];
    char payload[MSG_PAYLOAD_SIZE];
} Message;

volatile sig_atomic_t running = 1;

void signal_handler(int signum) {
    running = 0;
}

// Thread para recibir mensajes
void *receive_messages(void *arg) {
    int sock = *((int *)arg);
    Message msg;
    
    while (running) {
        // Recibir mensaje
        ssize_t bytes_read = read(sock, &msg, sizeof(msg));
        
        if (bytes_read <= 0) {
            if (errno == EINTR) continue; // Interrupción por señal
            if (bytes_read == 0) {
                printf("Broker cerró la conexión\n");
            } else {
                perror("Error al recibir mensaje");
            }
            running = 0;
            break;
        }
        
        // Mostrar el mensaje recibido
        printf("\n[Recibido de productor %d en topic '%s']: %s\n> ", 
               msg.producer_id, msg.topic, msg.payload);
        fflush(stdout);
    }
    
    return NULL;
}

int main(int argc, char *argv[]) {
    int sock = 0;
    struct sockaddr_in serv_addr;
    int consumer_id;
    char topic[64];
    pthread_t receive_thread;
    
    // Configurar manejador de señales
    signal(SIGINT, signal_handler);
    signal(SIGTERM, signal_handler);
    
    // Verificar argumentos
    if (argc < 3) {
        printf("Uso: %s <consumer_id> <topic>\n", argv[0]);
        return 1;
    }
    
    consumer_id = atoi(argv[1]);
    strncpy(topic, argv[2], sizeof(topic) - 1);
    topic[sizeof(topic) - 1] = '\0';
    
    printf("Consumidor %d iniciado para el topic '%s'\n", consumer_id, topic);
    
    // Crear socket
    if ((sock = socket(AF_INET, SOCK_STREAM, 0)) < 0) {
        perror("Error al crear socket");
        return 1;
    }
    
    // Configurar dirección del servidor
    serv_addr.sin_family = AF_INET;
    serv_addr.sin_port = htons(8080);
    
    // Convertir dirección IP
    if (inet_pton(AF_INET, "127.0.0.1", &serv_addr.sin_addr) <= 0) {
        perror("Dirección inválida");
        return 1;
    }
    
    // Conectar al servidor
    if (connect(sock, (struct sockaddr *)&serv_addr, sizeof(serv_addr)) < 0) {
        perror("Conexión fallida");
        return 1;
    }
    
    printf("Conectado al broker\n");
    
    // Enviar tipo de cliente (2 = consumidor)
    int client_type = 2;
    if (write(sock, &client_type, sizeof(client_type)) != sizeof(client_type)) {
        perror("Error al enviar tipo de cliente");
        close(sock);
        return 1;
    }
    
    // Enviar ID del consumidor
    if (write(sock, &consumer_id, sizeof(consumer_id)) != sizeof(consumer_id)) {
        perror("Error al enviar ID del consumidor");
        close(sock);
        return 1;
    }
    
    // Enviar topic para suscripción
    if (write(sock, topic, sizeof(topic)) != sizeof(topic)) {
        perror("Error al enviar topic de suscripción");
        close(sock);
        return 1;
    }
    
    printf("Suscrito al topic '%s'. Esperando mensajes...\n", topic);
    
    // Crear thread para recibir mensajes
    if (pthread_create(&receive_thread, NULL, receive_messages, &sock) != 0) {
        perror("Error al crear thread de recepción");
        close(sock);
        return 1;
    }
    
    // Interfaz para comandos del usuario
    printf("\nComandos disponibles:\n");
    printf("  quit - Salir del consumidor\n");
    
    char command[64];
    while (running) {
        printf("> ");
        if (fgets(command, sizeof(command), stdin) == NULL) {
            if (errno == EINTR) continue; // Interrupción por señal
            break;
        }
        
        // Eliminar el salto de línea
        size_t len = strlen(command);
        if (len > 0 && command[len-1] == '\n') {
            command[len-1] = '\0';
        }
        printf("Comando: %s\n", command);
        // Procesar comando
        if (strcmp(command, "quit") == 0) {
            running = 0;
            break;
        } else if (strlen(command) > 0) {
            printf("Comando desconocido: %s\n", command);
        }
    }
    
    // Esperar a que el thread termine
    running = 0;
    pthread_join(receive_thread, NULL);
    
    printf("Finalizando consumidor...\n");
    close(sock);
    return 0;
}