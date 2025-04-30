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
#pragma pack(push, 1)
typedef struct {
    long long id; // Message offset
    int producer_id;
    char topic[64];
    char payload[256];
} Message;
#pragma pack(pop)


//globales para el manejador de senales
volatile sig_atomic_t running = 1;
int sock = 0;
int consumer_id;
char topic[64];
char group_id[64] = "";



void send_unsubscribe_message() {
    struct linger so_linger = { .l_onoff = 1, .l_linger = 0 };
    setsockopt(sock, SOL_SOCKET, SO_LINGER, &so_linger, sizeof(so_linger));
    close(sock);
    printf("Desconectando del broker... ILUSION DE UNSUBSCRIBE????\n");
}


void signal_handler(int signum) {
    printf("\nRecibida señal de interrupción, cerrando...\n");
    send_unsubscribe_message();
    running = 0;
}

// Thread para recibir mensajes
void *receive_messages(void *arg) {
    printf("Inicio receive_messages\n");
    int sock = *((int *)arg);
    char buf[512];

    while (running) {
        printf("En while\n");

        // Leer datos del socket
        ssize_t n = read(sock, buf, sizeof(buf) - 1);
        if (n > 0) {
            buf[n] = '\0'; // Asegurar que la cadena esté terminada en NULL

            // Verificar si el mensaje es de control
            if (strncmp(buf, "SUBSCRIBED:", 11) == 0) {
                printf("Mensaje de control recibido: %s\n", buf);
            } else {
                // Es un mensaje real (payload)
                printf("Payload recibido: %s\n", buf);
            }
        } else if (n == 0) {
            printf("Broker cerró la conexión\n");
            running = 0;
            break;
        } else {
            perror("Error al recibir mensaje");
            printf("Error en read(), errno: %d\n", errno);
            running = 0;
            break;
        }
    }
    printf("fin receive_messages\n");


    return NULL;
}


// Add a flag to track if it's a new connection or reconnection
int main(int argc, char *argv[]) {
    struct sockaddr_in serv_addr;
    char group_id[64] = ""; // Initialize group_id to empty
    long long start_offset = -1; // -1 means start from latest messages
    pthread_t receive_thread;
    
    // Configure signal handler
    signal(SIGINT, signal_handler);
    signal(SIGTERM, signal_handler);
    
    // Verify arguments
    if (argc < 3) {
        printf("Usage: %s <consumer_id> <topic> [group_id] [start_offset]\n", argv[0]);
        printf("  [group_id] is optional - consumer group for load balancing\n");
        printf("  [start_offset] is optional - start consuming from this message ID\n");
        printf("    -1 = latest messages only\n");
        printf("     0 = all available messages (from beginning)\n");
        printf("     n = specific message ID\n");
        return 1;
    }
    
    consumer_id = atoi(argv[1]);
    strncpy(topic, argv[2], sizeof(topic) - 1);
    topic[sizeof(topic) - 1] = '\0';
    
    // Read the group_id if provided
    if (argc > 3) {
        strncpy(group_id, argv[3], sizeof(group_id) - 1);
        group_id[sizeof(group_id) - 1] = '\0';
    }
    
    // Read the start_offset if provided
    if (argc > 4) {
        start_offset = atoll(argv[4]);
    }
    
    if (strlen(group_id) > 0) {
        printf("Consumer %d started for topic '%s' in group '%s'", 
               consumer_id, topic, group_id);
    } else {
        printf("Consumer %d started for topic '%s' (no group)", 
               consumer_id, topic);
    }
    
    if (start_offset >= 0) {
        printf(" starting from offset %lld\n", start_offset);
    } else {
        printf(" starting from latest messages\n");
    }

    // Crear socket
if ((sock = socket(AF_INET, SOCK_STREAM, 0)) < 0) {
    perror("Error al crear socket");
    return 1;
}
printf("Socket creado correctamente\n");

// Configurar dirección del servidor
serv_addr.sin_family = AF_INET;
serv_addr.sin_port = htons(8080);

if (inet_pton(AF_INET, "127.0.0.1", &serv_addr.sin_addr) <= 0) {
    perror("Dirección inválida");
    return 1;
}
printf("Dirección configurada correctamente\n");

// Conectar al servidor
if (connect(sock, (struct sockaddr *)&serv_addr, sizeof(serv_addr)) < 0) {
    perror("Conexión fallida");
    return 1;
}
printf("Conexión al broker establecida correctamente\n");



    ///*************************/
    printf("Conectado al broker\n");

    // Enviar solicitud de suscripción con el formato esperado por el broker
    char subscribe_message[256]; // Increase size for offset
    
    if (start_offset >= 0) {
        snprintf(subscribe_message, sizeof(subscribe_message), 
         "SUBSCRIBE:%d:%s:%s:%lld", consumer_id, topic, group_id, start_offset);
    } else {
        snprintf(subscribe_message, sizeof(subscribe_message), 
         "SUBSCRIBE:%d:%s:%s", consumer_id, topic, group_id);
    }
    
    // Enviar solicitud de suscripción
    if (write(sock, subscribe_message, strlen(subscribe_message)) != strlen(subscribe_message)) {
        perror("Error al enviar solicitud de suscripción");
        close(sock);
        return 1;
    }
    printf("Solicitud de suscripción enviada correctamente\n");

    printf("Mensaje de suscripción: '%s'\n", subscribe_message);

    printf("Suscrito al topic '%s' (grupo '%s'). Esperando mensajes...\n", topic, group_id);

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

    // Procesar comando
    if (strcmp(command, "quit") == 0) {
        send_unsubscribe_message();
        running = 0;
        break;
    } else if (strlen(command) > 0) {
        printf("Comando desconocido: %s\n", command);
    }
        
}

    // Esperar a que el thread termine
    running = 0;
    pthread_cancel(receive_thread);  // Forzar termino del thread si está bloqueado en read()
    pthread_join(receive_thread, NULL);

    printf("Finalizando consumidor...\n");
    close(sock);
    return 0;
}