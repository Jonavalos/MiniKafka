#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <signal.h>
#include <errno.h>

#define MSG_PAYLOAD_SIZE 256

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

int main(int argc, char *argv[]) {
    int sock = 0;
    struct sockaddr_in serv_addr;
    int producer_id;
    char topic[64];
    long long seq = 0; // Contador de secuencia

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

    // Enviar 5 mensajes
    for (int i = 0; i < 5 && running; i++) {
        Message msg;
        msg.id = seq++;
        msg.producer_id = producer_id;
        strncpy(msg.topic, topic, sizeof(msg.topic) - 1);
        msg.topic[sizeof(msg.topic) - 1] = '\0';
        snprintf(msg.payload, sizeof(msg.payload), "Mensaje %lld", msg.id);

        if (write(sock, &msg, sizeof(msg)) != sizeof(msg)) {
            perror("Error al enviar mensaje");
            close(sock);
            return 1;
        }

        printf(">> Mensaje %lld enviado al topic '%s'\n", msg.id, topic);
        sleep(1); // Esperar 1 segundo entre mensajes
    }

    printf("Finalizando productor...\n");
    close(sock);
    return 0;
}



// int main(int argc, char *argv[]) {
//     int sock = 0;
//     struct sockaddr_in serv_addr;
//     int producer_id;
//     char topic[64];
    
//     // Configurar manejador de señales
//     signal(SIGINT, signal_handler);
//     signal(SIGTERM, signal_handler);
    
//     // Verificar argumentos
//     if (argc < 3) {
//         printf("Uso: %s <producer_id> <topic>\n", argv[0]);
//         return 1;
//     }
    
//     producer_id = atoi(argv[1]);
//     strncpy(topic, argv[2], sizeof(topic) - 1);
//     topic[sizeof(topic) - 1] = '\0';
    
//     printf("Productor %d iniciado para el topic '%s'\n", producer_id, topic);
    
//     // Crear socket
//     if ((sock = socket(AF_INET, SOCK_STREAM, 0)) < 0) {
//         perror("Error al crear socket");
//         return 1;
//     }
    
//     // Configurar dirección del servidor
//     serv_addr.sin_family = AF_INET;
//     serv_addr.sin_port = htons(8080);
    
//     // Convertir dirección IP
//     if (inet_pton(AF_INET, "127.0.0.1", &serv_addr.sin_addr) <= 0) {
//         perror("Dirección inválida");
//         return 1;
//     }
    
//     // Conectar al servidor
//     if (connect(sock, (struct sockaddr *)&serv_addr, sizeof(serv_addr)) < 0) {
//         perror("Conexión fallida");
//         return 1;
//     }
    
//     printf("Conectado al broker\n");
    
//     // Enviar tipo de cliente (1 = productor)
//     int client_type = 1;
//     if (write(sock, &client_type, sizeof(client_type)) != sizeof(client_type)) {
//         perror("Error al enviar tipo de cliente");
//         close(sock);
//         return 1;
//     }
    
//     // Enviar ID del productor
//     if (write(sock, &producer_id, sizeof(producer_id)) != sizeof(producer_id)) {
//         perror("Error al enviar ID del productor");
//         close(sock);
//         return 1;
//     }
    
//     printf("Produciendo mensajes. Presione Ctrl+C para salir.\n");
//     printf("Ingrese mensajes para enviar (presione Enter después de cada mensaje):\n");
    
//     // Bucle principal para enviar mensajes
//     char input[MSG_PAYLOAD_SIZE];
//     while (running) {
//         Message msg;
        
//         // Leer entrada del usuario
//         printf("> ");
//         if (fgets(input, sizeof(input), stdin) == NULL) {
//             if (errno == EINTR) continue; // Interrupción por señal
//             break;
//         }
        
//         // Eliminar el salto de línea
//         size_t len = strlen(input);
//         if (len > 0 && input[len-1] == '\n') {
//             input[len-1] = '\0';
//         }
        
//         // Si el mensaje está vacío, continuar
//         if (strlen(input) == 0) {
//             continue;
//         }
        
//         // Preparar el mensaje
//         msg.id = 0; // Inicializar el ID
//         msg.producer_id = producer_id;
//         strncpy(msg.topic, topic, sizeof(msg.topic) - 1);
//         msg.topic[sizeof(msg.topic) - 1] = '\0';
//         strncpy(msg.payload, input, sizeof(msg.payload) - 1);
//         msg.payload[sizeof(msg.payload) - 1] = '\0';

//         // Enviar el mensaje
//         if (write(sock, &msg, sizeof(msg)) != sizeof(msg)) {
//             perror("Error al enviar mensaje");
//             break;
//         }
        
//         printf("Mensaje enviado al topic '%s': %s\n", topic, input);
//     }
    
//     printf("Finalizando productor...\n");
//     close(sock);
//     return 0;
// }