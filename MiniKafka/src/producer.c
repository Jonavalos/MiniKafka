#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <signal.h>
#include <errno.h>
#include <time.h>      // Opcional: para nanosleep()

#define MSG_PAYLOAD_SIZE 256
#define MAX_MESSAGES_PER_SECOND 10

// Message structure (debe coincidir con la definición en el broker)
#pragma pack(push, 1)
typedef struct {
    long long id;
    int producer_id;
    char topic[64];
    char payload[256];
} Message;
#pragma pack(pop)


volatile sig_atomic_t running = 1;

void signal_handler(int signum) {
    running = 0;
}

// Función para enviar todos los bytes de un mensaje
ssize_t send_all(int sock, const void *buffer, size_t length) {
    size_t total_sent = 0;
    const char *ptr = buffer;

    while (total_sent < length) {
        ssize_t bytes_sent = send(sock, ptr + total_sent, length - total_sent, 0);
        if (bytes_sent <= 0) {
            if (errno == EINTR) continue; // Reintentar si la llamada fue interrumpida
            perror("Error al enviar mensaje");
            return -1; // Error
        }
        total_sent += bytes_sent;
    }
    printf("DEBUG: Enviados %zu bytes\n", total_sent);
    return total_sent;
}


int main(int argc, char *argv[]) {
    int sock = 0;
    struct sockaddr_in serv_addr;
    int producer_id;
    char topic[64];
    long long seq = 0;                 // Contador de secuencia

    // Configurar manejador de señales
    signal(SIGINT, signal_handler);
    signal(SIGTERM, signal_handler);

    // Verificar argumentos
    if (argc < 3) {
        printf("Uso: %s <producer_id> <topic>\n", argv[0]);
        return 1;
    }

    producer_id = atoi(argv[1]);
    strncpy(topic, argv[2], sizeof(topic) - 1);
    topic[sizeof(topic) - 1] = '\0';

    printf("Productor %d iniciado para el topic '%s'\n", producer_id, topic);

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

    // Enviar tipo de cliente (1 = productor)
    int client_type = 1;
    if (write(sock, &client_type, sizeof(client_type)) != sizeof(client_type)) {
        perror("Error al enviar tipo de cliente");
        close(sock);
        return 1;
    }

    // Enviar ID del productor
    if (write(sock, &producer_id, sizeof(producer_id)) != sizeof(producer_id)) {
        perror("Error al enviar ID del productor");
        close(sock);
        return 1;
    }

    printf("Produciendo mensajes automáticamente. Presione Ctrl+C para detener.\n");

    // Bucle principal: enviar secuencia numérica cada segundo
    sleep(2);  // Esperar 2 segundos antes de empezar a enviar mensajes
    int max_messages = 200; // Límite de mensajes a enviar
    int message_count = 0;

    if (running) {
        Message msg = {0}; // Inicializa todos los campos a 0
        msg.id = producer_id; // ID del mensaje
        msg.producer_id = producer_id;
        strncpy(msg.topic, topic, sizeof(msg.topic) - 1);
        msg.topic[sizeof(msg.topic) - 1] = '\0';
        snprintf(msg.payload, sizeof(msg.payload), "Mensaje del productor %d", producer_id);

        // Validar que el mensaje tenga un topic y un payload válidos
        if (strlen(msg.topic) == 0 || strlen(msg.payload) == 0) {
            fprintf(stderr, "ERROR: Mensaje no válido. Topic o payload vacío.\n");
            close(sock);
            return 1; // Salir con un código de error
        }
        printf("DEBUG: Enviando mensaje ID=%lld, ProducerID=%d, Topic='%s', Payload='%s'\n",
            msg.id, msg.producer_id, msg.topic, msg.payload);
        // Enviar el mensaje
        if (send_all(sock, &msg, sizeof(msg)) != sizeof(msg)) {
            perror("Error al enviar mensaje");
            close(sock);
            return 1; // Salir con un código de error
        }

        printf(">> Mensaje enviado al topic '%s' con payload: '%s'\n", msg.topic, msg.payload);
    }

    printf("Finalizando productor...\n");
    close(sock);
    return 0;
}

