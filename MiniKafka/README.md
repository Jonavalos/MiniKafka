
## Librerías Utilizadas

* `stdio.h`: Para operaciones de entrada/salida estándar como `printf`, `fprintf` y `perror`.
* `stdlib.h`: Para funciones de utilidad general como `malloc`, `free`, `atoi` y `exit`.
* `string.h`: Para manipulación de cadenas de caracteres como `strcpy`, `strncpy`, `strcmp` y `strlen`.
* `pthread.h`: Para la creación y gestión de threads (hilos), esencial para manejar múltiples conexiones de clientes concurrentemente.
* `fcntl.h`: Para operaciones de control de archivos, incluyendo la creación de archivos con permisos específicos (podría usarse para persistencia, aunque no se vea directamente en los includes).
* `sys/mman.h`: Para la gestión de memoria mapeada (`mmap`), que podría utilizarse para compartir datos entre procesos o para persistencia eficiente.
* `unistd.h`: Para funciones del sistema operativo como `sleep`, `close` y `unlink`.
* `stdbool.h`: Para el tipo de dato booleano (`bool`) y sus valores (`true`, `false`).
* `signal.h`: Para el manejo de señales del sistema, permitiendo una terminación limpia del broker.
* `time.h`: Para funciones relacionadas con el tiempo, como `time` y `nanosleep` (podría usarse para temporizadores o gestión de inactividad).
* `errno.h`: Para el manejo de errores a través de la variable global `errno`.
* `sys/socket.h`: Para la creación y manipulación de sockets, la base de la comunicación en red.
* `netinet/in.h`: Para las estructuras de direcciones de internet (IPv4 e IPv6).
* `arpa/inet.h`: Para funciones de conversión de direcciones IP (entre formato numérico y presentación).
* `stdarg.h`: Para funciones que aceptan un número variable de argumentos (como `vprintf` o una función de logging personalizada).
* `poll.h`: Para la función `poll`, que permite esperar eventos en múltiples descriptores de archivo (sockets) de manera eficiente.
* `asm-generic/socket.h`: Define constantes genéricas para sockets (generalmente incluido a través de `sys/socket.h`, su inclusión directa podría indicar un uso más específico de funcionalidades de bajo nivel de sockets).
* `sys/time.h`: Para estructuras y funciones relacionadas con el tiempo, como `gettimeofday` (para obtener la hora actual con alta precisión).
* `semaphore.h`: Para la creación y manipulación de semáforos, mecanismos de sincronización entre threads o procesos.

# 📦 Estructuras

## Message
Estructura que representa un mensaje producido por un cliente. Contiene un identificador único (id), el identificador del productor (producer_id), el nombre del tópico (topic) y el contenido del mensaje (payload). Está alineada con `#pragma pack(1)` para evitar relleno de memoria.

## message_history
Arreglo estático que almacena el historial de mensajes enviados, hasta un máximo definido por `MAX_MESSAGE_HISTORY`. Protegido por un mutex (`message_history_mutex`) para garantizar acceso concurrente seguro.

## MessageQueue
Cola circular de mensajes compartida entre productores y consumidores. Implementada con sincronización basada en mutex (`mutex`), condiciones (`not_empty`, `not_full`) y semáforos (`empty_slots`, `filled_slots`). Contiene una bandera de apagado (`shutdown`) y un contador de IDs (`next_message_id`) para asignar identificadores únicos a los mensajes.

## LogMessage
Estructura utilizada para representar un mensaje de log. Contiene una marca de tiempo (`timestamp`) y el contenido textual del mensaje (`message`), con longitud máxima definida por `LOG_MSG_SIZE`.

## LogQueue
Cola circular utilizada exclusivamente para el sistema de logging asincrónico. Sincronizada mediante mutex y variables de condición. Es consumida por un hilo dedicado (`logger_thread`) y soporta apagado controlado mediante la bandera `shutdown`.

## ConsumerSubscription
Nodo de una lista enlazada que representa a un consumidor suscrito. Contiene el identificador del consumidor (`consumer_id`), descriptor de socket (`socket_fd`), una lista de tópicos suscritos (`topics`) y la cantidad total de suscripciones (`topic_count`). Protegido por `subscription_mutex`.

## ConsumerOffset
Estructura que lleva el control del último offset consumido por un consumidor específico en un grupo y tópico determinado. Utilizada para mantener consistencia en el consumo por grupo. Forma parte de una lista enlazada protegida por `consumer_offsets_mutex`.

## ProdArg
Estructura auxiliar utilizada para pasar argumentos a hilos de manejo de productores. Contiene el descriptor de archivo (`fd`) y el identificador del productor (`producer_id`).

## GroupDistributionState
Estado de distribución de un grupo de consumidores para un tópico específico. Utilizado para realizar balanceo tipo round-robin en el envío de mensajes. Protegido mediante `group_distribution_state_mutex`.

## GroupMember
Nodo de una lista enlazada que representa un miembro de un grupo de consumidores. Contiene información del tópico y grupo al que pertenece, el identificador del consumidor y su socket. Protegido por `group_members_mutex`.

## TaskType
Enumeración que define los tipos de tarea manejadas por el thread pool: suscripción (`TASK_SUBSCRIBE`), desuscripción (`TASK_UNSUBSCRIBE`) y producción (`TASK_PRODUCE`).

## TaskData
Estructura que encapsula los datos asociados a una tarea. Incluye el tipo (`type`), el descriptor del cliente (`client_fd`), identificador del consumidor (`consumer_id`), tópico (`topic`), identificador de grupo (`group_id`) y el offset inicial desde el cual consumir (`start_offset`).

## TaskNode
Nodo de una lista enlazada que contiene una instancia de `TaskData`. Utilizado para implementar la cola de tareas del thread pool.

## TaskQueue
Cola sincronizada de tareas pendientes para el thread pool. Utiliza un mutex (`mutex`) y una variable de condición (`not_empty`) para coordinar el acceso entre múltiples hilos. Soporta apagado limpio mediante la bandera `shutdown`.

## ThreadPool
Contenedor del thread pool, que incluye un arreglo de hilos (`threads`) y una instancia de `TaskQueue`. Se encarga de gestionar las tareas concurrentes del broker sin bloquear la ejecución principal.

# 🧩 Métodos

## 🧵 THREAD POOL

### `signal_handler(int signum)`
Manejador de señales que permite la terminación controlada del broker. Cambia la bandera global `running` a 0, guarda los offsets de consumidores activos mediante `save_consumer_offsets()` y señala al hilo de logging para que finalice correctamente.

### `init_task_queue(TaskQueue *q)`
Inicializa una instancia de `TaskQueue`, configurando los punteros de cabeza y cola, la bandera de apagado (`shutdown`), el mutex y la variable de condición necesaria para la sincronización de acceso a la cola.

### `enqueue_task_data(TaskQueue *q, TaskData *td)`
Inserta una nueva tarea en la cola de tareas. Crea dinámicamente un nodo `TaskNode`, lo enlaza al final de la cola y señala a los hilos trabajadores mediante `pthread_cond_signal` para que procesen la nueva tarea.

### `dequeue_task(TaskQueue *q)`
Extrae una tarea de la cola de tareas. Si la cola está vacía, espera con `pthread_cond_wait`. Retorna `NULL` si la cola está en modo apagado (`shutdown`). Garantiza acceso seguro usando mutex.

### `worker_loop(void *arg)`
Función principal ejecutada por los hilos del thread pool. Obtiene tareas de la cola y ejecuta su lógica según el tipo (`TASK_SUBSCRIBE`, `TASK_UNSUBSCRIBE`, etc.). Libera los nodos procesados y finaliza cuando `dequeue_task` devuelve `NULL`.

### `init_thread_pool(ThreadPool *p)`
Inicializa el thread pool, configurando la cola de tareas y lanzando `THREAD_POOL_SIZE` hilos que ejecutan la función `worker_loop`.

### `shutdown_thread_pool(ThreadPool *p)`
Detiene ordenadamente todos los hilos del thread pool. Marca la cola como cerrada (`shutdown = true`), despierta a todos los hilos esperando tareas y espera a que terminen con `pthread_join`. Luego, libera recursos sincronizados (mutex y condición).

## 🍟 GROUPING

### `add_group_member(const char *topic, const char *group_id, int consumer_id, int socket_fd)`
Agrega un consumidor a un grupo de consumidores asociado a un tópico específico. Inserta un nuevo nodo `GroupMember` en la lista enlazada `group_members`, protegiendo el acceso concurrente con `group_members_mutex`.

### `remove_group_member(const char *topic, const char *group_id, int consumer_id)`
Elimina a un consumidor específico de un grupo de un tópico dado. Si el grupo queda vacío tras la eliminación, limpia el estado de distribución (`GroupDistributionState`) asociado al grupo. Protege la operación con `group_members_mutex`.

### `remove_group_distribution_state(const char *topic, const char *group_id)`
Elimina el estado de distribución de mensajes (`GroupDistributionState`) para un grupo dado. Esto asegura que ya no se realicen rondas de distribución para ese grupo en particular. El acceso está sincronizado con `group_distribution_state_mutex`.

### `handle_consumer_disconnect(int consumer_id)`
Gestiona la desconexión lógica de un consumidor, eliminándolo de todos los grupos/tópicos en los que esté registrado. Esta operación reutiliza `remove_group_member` y `remove_subscription`, garantizando una limpieza completa del estado del consumidor.

### `handle_socket_disconnect(int socket_fd)`
Identifica el `consumer_id` asociado a un `socket_fd` y delega el manejo completo de desconexión a `handle_consumer_disconnect`. También cierra el socket al finalizar. Esta función asegura la liberación ordenada de recursos ante desconexiones abruptas.

## 📡 SUBSCRIPTIONS

### `process_subscription_request(int consumer_id, int socket_fd, const char *topic, const char *group_id, long long start_offset)`
Procesa la solicitud inicial de suscripción de un consumidor. Establece el offset de inicio: si `start_offset == -1`, se utiliza el offset más reciente disponible. Luego, registra al consumidor en el grupo correspondiente mediante `add_group_member`.

### `add_subscription(int consumer_id, int socket_fd, const char *topic, const char *group_id)`
Agrega un tópico a la lista de suscripciones activas de un consumidor. Si no existe una suscripción previa, se crea una nueva entrada `ConsumerSubscription`. También se asegura que el consumidor esté registrado en el grupo con `add_group_member` y se actualiza su offset al siguiente mensaje disponible (`last_offset + 1`). Sincronizado mediante `subscription_mutex`.

### `remove_subscription(int consumer_id, const char *topic)`
Elimina un tópico específico de la lista de suscripciones del consumidor. Si, tras la eliminación, el consumidor ya no tiene suscripciones activas, se libera completamente su estructura `ConsumerSubscription`. Protegido por `subscription_mutex` para evitar condiciones de carrera.

## 📨 MESSAGING

### `static int find_consumer_id_by_socket(int sock, const char *topic, const char *group_id)`
Busca y retorna el `consumer_id` asociado a un socket específico dentro de un grupo y tópico dados, protegiendo el acceso con mutex.

### `int get_next_consumer_in_group(const char *topic, const char *group_id)`
Selecciona el socket del próximo consumidor en un grupo de consumidores usando round-robin, manteniendo estado entre llamadas y asegurando sincronización mediante mutex.

### `void *message_processor_thread(void *arg)`
Hilo que procesa mensajes de la cola, determina a qué grupo pertenecen y delega la distribución al método correspondiente.

### `void distribute_message(const char *topic, const char *message)`
Distribuye un mensaje a todos los grupos suscritos a un tópico, seleccionando consumidores disponibles mediante round-robin y reintentando envíos fallidos hasta un máximo predefinido.

### `int init_msg_queue()`
Inicializa la cola de mensajes compartida en memoria usando `shm_open`, semáforos POSIX y un mutex compartido entre procesos.

### `void enqueue_message(int producer_id, const char *topic, const char *payload)`
Encola un nuevo mensaje en la cola circular compartida, sincronizando el acceso con semáforos y mutex.

### `bool dequeue_message(Message *out_msg)`
Extrae un mensaje de la cola compartida, sincronizando el acceso con semáforos y mutex, y lo guarda en el historial para trazabilidad.

### `void cleanup_msg_queue()`
Libera correctamente los recursos del sistema asociados a la cola de mensajes en memoria compartida, destruyendo los mutex y variables de condición, desmapeando la memoria y desvinculando el objeto shm.

### `void *connection_handler_thread(void *arg)`
Función principal de escucha para nuevas conexiones de clientes. Acepta conexiones, determina si son consumidores o productores mediante "peek" o handshake binario, y lanza el procesamiento correspondiente, ya sea agregando una tarea a la cola o creando un hilo para productores.

### `static ssize_t read_n_bytes(int fd, void *buf, size_t count)`
Lee exactamente `count` bytes desde un descriptor de archivo `fd` al búfer `buf`, manejando casos donde `read()` retorna menos de lo solicitado. Devuelve el total de bytes leídos o error.

### `void *producer_handler_thread_fd(void *arg)`
Hilo dedicado al manejo de un productor. Lee estructuras `Message` completas del socket, valida sus campos y las encola para su procesamiento, desconectando al productor en caso de errores o finalización.

## 🖋️ LOGGING

### `void *log_writer_thread(void *arg)`
Hilo dedicado a escribir mensajes de log desde la cola en memoria compartida hacia el archivo `broker.log`, manejando sincronización con mutex y condiciones hasta que el sistema se apague y la cola esté vacía.

### `int init_log_queue()`
Inicializa la cola de logs en memoria compartida (shm), configurando sus atributos de sincronización compartidos entre procesos, e inicializando su estado interno (punteros de cabeza, cola y contador).

### `void enqueue_log(const char *format, ...)`
Agrega un nuevo mensaje formateado a la cola de logs con timestamp actual, esperando si la cola está llena y señalando a los consumidores una vez insertado, usando sincronización con mutex y variables de condición.

### `void shutdown_logger()`
Solicita un apagado ordenado del sistema de logging, activando la bandera de cierre (`shutdown`), notificando al hilo escritor y esperando a que termine antes de liberar los recursos.

### `void cleanup_log_queue()`
Libera los recursos del sistema asociados a la cola de logs: destruye los mutex y condiciones, desmapea la memoria y elimina el segmento compartido (`shm_unlink`).

## 🪧 OFFSETS

### `void update_consumer_offset(const char *topic, const char *group_id, int consumer_id, long long message_id)`
Actualiza o crea el registro del último offset consumido por un consumidor específico dentro de un grupo y tópico, usando una lista enlazada protegida con mutex.

### `long long get_last_consumer_offset(const char *topic, const char *group_id, int consumer_id)`
Retorna el último offset consumido por un consumidor específico en un grupo y tópico, o -1 si no existe registro.

### `long long get_group_last_offset(const char *topic, const char *group_id)`
Obtiene el offset más alto consumido por cualquier consumidor de un grupo específico en un tópico determinado.

### `void save_consumer_offsets()`
Guarda en el archivo `consumer_offsets.dat` todos los offsets consumidos registrados en la lista enlazada, para persistencia entre ejecuciones.

### `void load_consumer_offsets()`
Carga desde el archivo `consumer_offsets.dat` los offsets guardados, reconstruyendo la lista enlazada en memoria.

### `void send_messages_from_offset(int consumer_id, int client_fd, const char *topic, const char *group_id, long long start_offset)`
Envía al consumidor los mensajes históricos de un tópico a partir de un offset específico, recorriendo el buffer circular en memoria.

### `void store_message_in_history(const Message *msg)`
Guarda un mensaje en el historial de mensajes en memoria (con buffer circular), eliminando el más antiguo si se alcanza la capacidad máxima.

# 🐯 MAIN

Manejo de señales para un apagado limpio  
- Configura el manejo de señales (SIGINT, SIGTERM) para permitir una terminación ordenada del proceso, asegurando que todos los recursos se liberen correctamente.

Inicialización de la cola de logs  
- Llama a `init_log_queue()` para configurar la cola de logs que se utilizará en el hilo de registro. Si falla, se imprime un mensaje de error y el programa termina.

Inicialización de la cola de mensajes  
- Llama a `init_msg_queue()` para configurar la cola que manejará los mensajes entre los productores y consumidores. Si falla, se imprime un error, se limpian los recursos de logs, y el programa termina.

Carga de los offsets de consumidores previos  
- Llama a `load_consumer_offsets()` para cargar los offsets de los consumidores desde un archivo persistido previamente. Esto permite que el broker sepa desde qué mensaje debe empezar a enviar a cada consumidor.

Inicialización del grupo de hilos (thread pool)  
- Se inicializa un pool de hilos para manejar las operaciones de los consumidores (SUBSCRIBE, UNSUBSCRIBE, PRODUCE). Este paso permite que múltiples operaciones se manejen simultáneamente.

Creación del hilo de escritura de logs  
- Se crea un hilo que ejecuta la función `log_writer_thread`, encargado de escribir los mensajes de log en un archivo. Si no puede crearse el hilo, se limpian los recursos y el programa termina.

Creación del hilo para el procesamiento de mensajes  
- Se crea un hilo que ejecuta la función `message_processor_thread`, encargado de procesar los mensajes entrantes de los productores y enviarlos a los consumidores. Si no puede crearse el hilo, se realiza el apagado ordenado de los recursos y el programa termina.

Configuración del socket del broker  
- Se configura un socket TCP para el broker que escuchará conexiones entrantes de los clientes (productores y consumidores).  
- Se configuran opciones como SO_REUSEADDR y SO_REUSEPORT para permitir la reutilización de direcciones y puertos.  
- Se vincula el socket al puerto 8080 y se pone a la espera de conexiones.

Creación del hilo de aceptación de conexiones  
- Se crea un hilo que ejecutará la función `connection_handler_thread`, encargada de aceptar nuevas conexiones y procesarlas. Si no se puede crear el hilo, se cierran los recursos del servidor y el programa entra en la sección de limpieza.

Esperar la señal de apagado  
- El programa entra en un bucle de espera, manteniéndose activo hasta recibir una señal de apagado (SIGINT o SIGTERM).

Apagado ordenado  
- En caso de recibir la señal de apagado, se realiza un apagado ordenado de todos los recursos:  
  - Se cierra el socket de aceptación de conexiones.  
  - Se apaga el thread pool, liberando los hilos de trabajo.  
  - Se apaga el hilo de escritura de logs.  
  - Se señala al procesador de mensajes que debe detenerse, notificando a través de la cola de mensajes.  
  - Se esperan a que todos los hilos terminen mediante `pthread_join`.  
- Finalmente, se limpian las colas de mensajes y logs.

Manejo de errores  
- Si alguna de las operaciones de inicialización falla, se procede a limpiar los recursos previamente asignados antes de salir del programa con un código de error. En caso de éxito, se termina correctamente con un valor de retorno 0.

Este flujo garantiza que el broker maneje de manera eficiente y segura la comunicación con los productores y consumidores, manteniendo un manejo robusto de los recursos y permitiendo una desconexión ordenada cuando sea necesario.












# Producer del Message Broker

Este es un productor simple para un sistema de message broker. Se conecta al broker, especifica un tema, y luego envía un único mensaje predefinido a ese tema.

## Funcionalidades

* **Especificación de Tema:** Permite definir el tema al cual el productor enviará el mensaje al iniciar.
* **Identificación del Productor:** Envía un ID único para identificarse ante el broker.
* **Envío de Mensaje Único:** Envía un único mensaje con un payload predefinido al tema especificado.
* **Manejo de Señales:** Implementa un manejador de señales para cerrar la conexión con el broker de manera limpia al recibir señales de interrupción (`SIGINT`) o terminación (`SIGTERM`).
* **Función de Envío Segura:** Utiliza una función (`send_all`) para asegurar que todos los bytes del mensaje se envíen correctamente.

## Cómo Compilar y Ejecutar

1.  **Guardar el código:** Guarda el código fuente en un archivo llamado `producer.c`.
2.  **Compilar:** Abre una terminal y utiliza un compilador de C (como GCC) para compilar el código:
    ```bash
    gcc producer.c -o producer
    ```
3.  **Ejecutar:** Ejecuta el productor desde la terminal, proporcionando el ID del productor y el tema al que deseas enviar el mensaje:
    ```bash
    ./producer <producer_id> <topic>
    ```
    * `<producer_id>`: Un número entero que identifica a este productor.
    * `<topic>`: El nombre del tema al que deseas enviar el mensaje.

    **Ejemplos de ejecución:**
    * Productor con ID 1 enviando al tema "noticias":
        ```bash
        ./producer 1 noticias
        ```
    * Productor con ID 2 enviando al tema "eventos":
        ```bash
        ./producer 2 eventos
        ```

## Detalles del Código

* **`#include`s:** Incluye las bibliotecas necesarias para operaciones de entrada/salida, manejo de memoria, manipulación de strings, sockets, redes y señales. La inclusión de `<time.h>` es actualmente opcional ya que `nanosleep()` no se utiliza en la versión actual del código.
* **`MSG_PAYLOAD_SIZE`:** Define el tamaño máximo del payload de un mensaje.
* **`MAX_MESSAGES_PER_SECOND`:** Define un límite en la frecuencia de envío de mensajes (actualmente no se aplica en la lógica principal de envío único).
* **`Message` struct:** Define la estructura de un mensaje, que debe coincidir con la definición en el broker. Incluye el ID del mensaje, el ID del productor, el tema y el payload. Se utiliza `#pragma pack(push, 1)` y `#pragma pack(pop)` para asegurar que la estructura se empaquete sin padding, lo cual es crucial para la comunicación binaria a través de la red.
* **Variables Globales:**
    * `running`: Un flag volátil para controlar el bucle principal (aunque en este productor de envío único, su uso es principalmente para la respuesta a señales).
* **`signal_handler()`:** Función que se ejecuta cuando se recibe una señal `SIGINT` o `SIGTERM`. Establece `running` en 0 para indicar que el programa debe finalizar.
* **`send_all()`:** Función para enviar todos los bytes de un buffer a través del socket. Intenta reenviar los bytes restantes en caso de interrupción (`EINTR`). Devuelve el número total de bytes enviados o -1 en caso de error. Se incluye un `printf` de depuración para mostrar la cantidad de bytes enviados.
* **`main()`:**
    * **Manejo de Argumentos:** Analiza los argumentos de la línea de comandos para obtener el ID del productor y el tema.
    * **Configuración del Socket:** Crea un socket TCP.
    * **Configuración de la Dirección del Servidor:** Establece la dirección IP y el puerto del broker (actualmente codificados como `127.0.0.1:8080`).
    * **Conexión al Broker:** Intenta establecer una conexión con el broker.
    * **Envío del Tipo de Cliente:** Envía un entero `1` al broker para indicar que este cliente es un productor.
    * **Envío del ID del Productor:** Envía el ID del productor al broker.
    * **Creación y Envío del Mensaje:**
        * Inicializa una estructura `Message`.
        * Asigna el ID del productor al campo `id` del mensaje (podría ser un identificador de mensaje único generado de otra manera en un productor más complejo).
        * Establece el `producer_id`.
        * Copia el tema proporcionado por el usuario al campo `topic`.
        * Define un payload estático: `"Mensaje del productor <producer_id>"`.
        * Realiza una validación básica para asegurar que el `topic` y el `payload` no estén vacíos.
        * Utiliza la función `send_all()` para enviar la estructura `Message` completa al broker.
    * **Finalización:** Cierra el socket después de enviar el mensaje (o si ocurre un error).

## Consideraciones

* **Envío Único:** Este productor está diseñado para enviar un único mensaje y luego finalizar. Un productor más complejo podría enviar múltiples mensajes en un bucle continuo o responder a eventos externos.
* **Contador de Secuencia:** La variable `seq` está declarada pero no se utiliza en la lógica actual de envío único. En un productor que envía múltiples mensajes, podría usarse para generar IDs de mensaje secuenciales.
* **Límite de Frecuencia:** La constante `MAX_MESSAGES_PER_SECOND` está definida pero no se aplica en la lógica de envío único. Sería relevante en un productor que envía mensajes de forma continua para evitar sobrecargar el broker.
* **Manejo de Errores:** El código incluye cierto manejo de errores (por ejemplo, al crear el socket, conectar y enviar), pero podría mejorarse para ser más robusto.
* **Formato del Mensaje:** El productor envía la estructura `Message` completa al broker. El broker debe estar preparado para recibir y procesar mensajes en este formato binario.
* **Confirmaciones (Opcional):** Para una mayor fiabilidad, un productor más avanzado podría esperar confirmaciones del broker para asegurar que los mensajes se han recibido correctamente.














# Consumer

Este es un consumidor simple para un sistema de message broker. Se conecta al broker, se suscribe a un tema específico (opcionalmente dentro de un grupo de consumidores), y recibe los mensajes publicados en ese tema.

## Funcionalidades

* **Suscripción a Temas:** Permite especificar el tema al cual el consumidor desea suscribirse al iniciar.
* **Grupos de Consumidores (Opcional):** Soporta la pertenencia a un grupo de consumidores, lo que permite el balanceo de carga de mensajes entre los consumidores del mismo grupo.
* **Consumo desde Offset Específico (Opcional):** Permite especificar un offset desde el cual comenzar a consumir mensajes. Las opciones incluyen:
    * `-1`: Consumir solo los mensajes más recientes publicados después de la suscripción.
    * `0`: Consumir todos los mensajes disponibles para el tema desde el inicio.
    * `n`: Consumir a partir del mensaje con el ID (offset) `n`.
* **Recepción Asíncrona:** Utiliza un thread separado para recibir mensajes del broker, lo que permite que el programa principal siga funcionando y responda a comandos del usuario.
* **Manejo de Señales:** Implementa un manejador de señales para cerrar la conexión con el broker de manera limpia al recibir señales de interrupción (`SIGINT`) o terminación (`SIGTERM`).
* **Comando de Salida:** Proporciona un comando (`quit`) para desconectar del broker y finalizar el consumidor de forma controlada.

## Cómo Compilar y Ejecutar

1.  **Guardar el código:** Guarda el código fuente en un archivo llamado `consumer.c`.
2.  **Compilar:** Abre una terminal y utiliza un compilador de C (como GCC) para compilar el código:
    ```bash
    gcc consumer.c -o consumer -pthread
    ```
    El flag `-pthread` es necesario para habilitar el soporte de threads.
3.  **Ejecutar:** Ejecuta el consumidor desde la terminal, proporcionando el ID del consumidor y el tema al que deseas suscribirte. Opcionalmente, puedes especificar un `group_id` y un `start_offset`:
    ```bash
    ./consumer <consumer_id> <topic> [group_id] [start_offset]
    ```
    * `<consumer_id>`: Un número entero que identifica a este consumidor.
    * `<topic>`: El nombre del tema al que deseas suscribirte.
    * `[group_id]` (opcional): El ID del grupo de consumidores al que pertenece este consumidor.
    * `[start_offset]` (opcional): El offset del mensaje desde el cual comenzar a consumir.

    **Ejemplos de ejecución:**
    * Consumidor con ID 1, suscrito al tema "noticias":
        ```bash
        ./consumer 1 noticias
        ```
    * Consumidor con ID 2, suscrito al tema "eventos" y perteneciente al grupo "grupo-a":
        ```bash
        ./consumer 2 eventos grupo-a
        ```
    * Consumidor con ID 3, suscrito al tema "temperaturas" y consumiendo desde el inicio:
        ```bash
        ./consumer 3 temperaturas "" 0
        ```
    * Consumidor con ID 4, suscrito al tema "logs" y consumiendo solo los mensajes más recientes:
        ```bash
        ./consumer 4 logs grupo-b -1
        ```

## Detalles del Código

* **`#include`s:** Incluye las bibliotecas necesarias para operaciones de entrada/salida, manejo de memoria, manipulación de strings, sockets, redes, señales y threads.
* **`MSG_PAYLOAD_SIZE`:** Define el tamaño máximo del payload de un mensaje.
* **`Message` struct:** Define la estructura de un mensaje, que debe coincidir con la definición en el broker. Incluye el ID del mensaje (offset), el ID del productor, el tema y el payload. Se utiliza `#pragma pack(push, 1)` y `#pragma pack(pop)` para asegurar que la estructura se empaquete sin padding, lo cual es crucial para la comunicación binaria a través de la red.
* **Variables Globales:**
    * `running`: Un flag volátil para controlar el bucle principal y los threads.
    * `sock`: El descriptor del socket de conexión con el broker.
    * `consumer_id`: El ID de este consumidor.
    * `topic`: El tema al que está suscrito este consumidor.
    * `group_id`: El ID del grupo de consumidores (puede estar vacío).
* **`send_unsubscribe_message()`:** Función para enviar una solicitud de "desuscripción" al broker (implementada como un cierre de socket con `SO_LINGER` para descartar cualquier dato pendiente).
* **`signal_handler()`:** Función que se ejecuta cuando se recibe una señal `SIGINT` o `SIGTERM`. Cierra la conexión con el broker y establece `running` en 0 para finalizar el programa.
* **`receive_messages()`:** Función que se ejecuta en un thread separado. Lee continuamente los mensajes del socket y los imprime en la consola. Maneja el cierre de la conexión por parte del broker y errores de lectura.
* **`main()`:**
    * **Manejo de Argumentos:** Analiza los argumentos de la línea de comandos para obtener el ID del consumidor, el tema, el grupo (opcional) y el offset de inicio (opcional).
    * **Configuración del Socket:** Crea un socket TCP.
    * **Configuración de la Dirección del Servidor:** Establece la dirección IP y el puerto del broker (actualmente codificados como `127.0.0.1:8080`).
    * **Conexión al Broker:** Intenta establecer una conexión con el broker.
    * **Envío de Solicitud de Suscripción:** Envía un mensaje al broker con el formato `SUBSCRIBE:<consumer_id>:<topic>:<group_id>[:start_offset]` para indicar el tema al que desea suscribirse (incluyendo el grupo y el offset si se proporcionaron).
    * **Creación del Thread de Recepción:** Crea un nuevo thread que ejecuta la función `receive_messages` para recibir los mensajes del broker de forma asíncrona.
    * **Interfaz de Comandos del Usuario:** Entra en un bucle donde espera comandos del usuario desde la entrada estándar. Actualmente, solo se admite el comando `quit` para salir.
    * **Finalización:** Cuando el usuario ingresa `quit` o se recibe una señal de interrupción, se establece `running` en 0, se espera a que el thread de recepción termine y se cierra el socket.

## Consideraciones

* **Manejo de Errores:** El código incluye cierto manejo de errores (por ejemplo, al crear el socket, conectar, enviar la suscripción, recibir mensajes y crear el thread), pero podría mejorarse para ser más robusto.
* **Protocolo de Comunicación:** Este consumidor asume un protocolo de comunicación simple basado en strings para la suscripción y mensajes sin formato para el payload. Un protocolo más estructurado (por ejemplo, usando JSON o un formato binario específico) podría ser más eficiente y flexible.
* **Desuscripción:** La "desuscripción" se implementa simplemente cerrando la conexión. Un protocolo más completo podría incluir un mensaje explícito de desuscripción al broker.
* **Reconexión:** El consumidor no implementa la lógica de reconexión automática en caso de que la conexión con el broker se pierda.
* **Serialización/Deserialización:** Los mensajes recibidos se imprimen directamente como strings. Si el broker enviara mensajes en un formato binario (basado en la estructura `Message`), sería necesario deserializarlos correctamente en el consumidor. El código actual asume que el broker envía el payload directamente como un string.
* **Formato del Mensaje Recibido:** El thread de recepción actualmente espera recibir el payload del mensaje directamente como un string. Si el broker enviara la estructura `Message` completa, la lógica de recepción tendría que deserializarla para acceder al payload.