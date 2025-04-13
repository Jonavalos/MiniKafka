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
    long long id;
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
    char group_id[64] = ""; // Inicializar group_id a vacío
    pthread_t receive_thread;

    // Configurar manejador de señales
    signal(SIGINT, signal_handler);
    signal(SIGTERM, signal_handler);

    // Verificar argumentos
    if (argc < 3) {
        printf("Uso: %s <consumer_id> <topic> [group_id]\n", argv[0]);
        printf("  [group_id] es opcional.\n");
        return 1;
    }

    consumer_id = atoi(argv[1]);
    strncpy(topic, argv[2], sizeof(topic) - 1);
    topic[sizeof(topic) - 1] = '\0';

    // Leer el group_id si se proporciona
    if (argc > 3) {
        strncpy(group_id, argv[3], sizeof(group_id) - 1);
        group_id[sizeof(group_id) - 1] = '\0';
        printf("Consumidor %d iniciado para el topic '%s' en el grupo '%s'\n", consumer_id, topic, group_id);
    } else {
        printf("Consumidor %d iniciado para el topic '%s' (sin grupo)\n", consumer_id, topic);
    }

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

    ///*************************/
    printf("Conectado al broker\n");

    // Enviar solicitud de suscripción con el formato esperado por el broker
    char subscribe_message[192]; // Aumentar el tamaño para el group_id
    snprintf(subscribe_message, sizeof(subscribe_message), "SUBSCRIBE:%d:%s:%s", consumer_id, topic, group_id);

    if (write(sock, subscribe_message, strlen(subscribe_message)) != strlen(subscribe_message)) {
        perror("Error al enviar solicitud de suscripción");
        close(sock);
        return 1;
    }

    printf("Enviada solicitud de suscripción: '%s'\n", subscribe_message);

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
        printf("Comando: %s\n", command);
        // fflush(stdout);
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