## Descripción General del Programa

Este programa implementa un sistema de message broker, un componente esencial en arquitecturas de software que habilita la comunicación asíncrona entre diferentes servicios o aplicaciones. El broker actúa como intermediario, recibiendo mensajes de los "productores", almacenándolos temporalmente y entregándolos a los "consumidores" interesados en esos mensajes. Este enfoque de comunicación desacopla a los productores y consumidores, permitiéndoles operar de forma independiente y a diferentes velocidades.

## Funciones Principales y Aspectos Técnicos Clave

1.  **Manejo de Conexiones de Clientes:**

    * El broker utiliza sockets TCP para establecer y gestionar las conexiones con productores y consumidores.
    * Para manejar la concurrencia y atender a múltiples clientes simultáneamente, el broker emplea un thread pool.
    * El acceso a la cola de tareas del thread pool se sincroniza mediante mutexes y variables de condición, garantizando la seguridad en la manipulación de las tareas (suscripciones, desuscripciones, producción de mensajes).

2.  **Gestión de Mensajes:**

    * Los mensajes se almacenan en una cola compartida en memoria (`msg_queue`).
    * El acceso concurrente a esta cola se controla con semáforos y mutexes, previniendo condiciones de carrera y manteniendo la integridad de los datos.
    * Además, el broker mantiene un historial de mensajes recientes, protegido por un mutex, para permitir a los nuevos consumidores recuperar mensajes anteriores.

3.  **Suscripciones y Grupos de Consumidores:**

    * Los consumidores pueden suscribirse a temas específicos para recibir los mensajes correspondientes.
    * El broker soporta la agrupación de consumidores, donde los mensajes de un tema se distribuyen entre los consumidores de un mismo grupo, facilitando el balanceo de carga y la escalabilidad.
    * La gestión de suscripciones y la información de los grupos se realiza mediante listas enlazadas, con la concurrencia controlada por mutexes.

4.  **Manejo de Offsets de Consumo:**

    * El broker realiza un seguimiento del último mensaje consumido por cada consumidor dentro de un grupo y tema.
    * Este seguimiento es fundamental para garantizar la entrega de mensajes al menos una vez y para permitir a los consumidores reanudar el consumo desde el punto donde se detuvieron.
    * Los offsets de consumo se almacenan y cargan desde un archivo, proporcionando persistencia.

5.  **Logging:**

    * El broker incluye un sistema de logging asíncrono para registrar eventos y errores.
    * Los mensajes de registro se encolan y se escriben en un archivo por un hilo dedicado, evitando bloqueos en el flujo principal del programa.
    * La sincronización en este sistema de logging también se logra mediante mutexes y variables de condición.

6.  **Manejo de Señales:**

    * El broker implementa el manejo de señales del sistema (como `SIGINT` y `SIGTERM`) para realizar un apagado limpio y ordenado.
    * Este proceso incluye guardar los offsets de los consumidores, liberar recursos y notificar a los hilos en ejecución para que terminen su trabajo.

**Resumen:**

Este sistema de message broker es una solución robusta que emplea técnicas avanzadas de programación concurrente y de sistemas, incluyendo el uso extensivo de threads, mutexes, variables de condición, semáforos y memoria compartida. Estas técnicas permiten al broker proporcionar una comunicación eficiente, confiable y escalable entre productores y consumidores.

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

### Script para Pruebas de Producers (`test_producers.sh`)

Este script de Bash está diseñado para compilar el programa `producer.c` y lanzar múltiples instancias de productores, cada uno con un ID único y publicando mensajes a un tema especificado por el usuario.

#### Funcionalidades

* **Compilación Automática:** Compila el archivo fuente `producer.c` utilizando `gcc` y la librería `pthread`.
* **Lanzamiento Múltiple de Producers:** Inicia un número predefinido de productores (actualmente configurado para lanzar 100 productores con IDs del 1 al 100).
* **Especificación de Tema por Argumento:** Permite especificar el tema al que todos los productores publicarán como un argumento de línea de comandos.
* **Asignación Automática de IDs:** Asigna a cada productor un ID único secuencialmente.
* **Manejo de Señales:** Implementa un mecanismo de limpieza para detener todos los procesos de productor en ejecución al recibir las señales `SIGINT` (Ctrl+C) o `SIGTERM`.
* **Espera de Finalización:** El script principal espera hasta que se reciba una señal de interrupción para finalizar, manteniendo los productores en ejecución hasta entonces.

#### Cómo Utilizar

1.  **Guardar el script:** Guarda el código del script en un archivo llamado `test_producers.sh` (o con el nombre que prefieras).
2.  **Otorgar permisos de ejecución:** Abre una terminal y otorga permisos de ejecución al script:
    ```bash
    chmod +x test_producers.sh
    ```
3.  **Asegurarse de tener `producer.c`:** Asegúrate de que el archivo fuente del productor (`producer.c`) se encuentre en el directorio `../src/` relativo a la ubicación del script, tal como se especifica en la línea de compilación.
4.  **Ejecutar el script:** Ejecuta el script desde la terminal, proporcionando el tema al que deseas que los productores publiquen:
    ```bash
    ./test_producers.sh <topic>
    ```
    Reemplaza `<topic>` con el nombre del tema deseado (por ejemplo, `noticias`, `eventos`, `temperaturas`).

#### Configuración

La siguiente variable en el script puede ser modificada para ajustar el comportamiento de las pruebas:

* El bucle `for PRODUCER_ID in {1..100}` controla el número de productores que se lanzarán y sus IDs. Puedes modificar el rango `{1..100}` para lanzar más o menos productores con diferentes rangos de IDs.
* `TOPIC=$1`: Esta variable toma el primer argumento pasado al script y lo utiliza como el tema para todos los productores.

#### Funcionamiento

1.  El script comienza estableciendo opciones seguras de Bash (`set -euo pipefail`).
2.  Intenta compilar el archivo `../src/producer.c` utilizando `gcc` y enlazando la librería `pthread`. Si la compilación falla, se muestra un error y el script se detiene.
3.  Verifica si se ha proporcionado exactamente un argumento de línea de comandos (el tema). Si no es así, muestra un mensaje de uso y se detiene.
4.  El tema proporcionado se almacena en la variable `TOPIC`.
5.  Se inicializa un array vacío (`PIDS`) para almacenar los IDs de proceso (PIDs) de los productores lanzados.
6.  Se define una función `cleanup()` que se ejecutará al recibir las señales `SIGINT` o `SIGTERM`. Esta función itera sobre los PIDs almacenados y envía una señal `kill` a cada proceso de productor para detenerlos.
7.  Se establece un `trap` para que la función `cleanup()` se ejecute al recibir las señales mencionadas.
8.  El script procede a lanzar 100 productores.
9.  Dentro de un bucle, para cada ID de productor del 1 al 100:
    * Se muestra un mensaje indicando el ID del productor que se está iniciando.
    * Se ejecuta el programa `producer` en segundo plano (`&`), pasándole el ID del productor y el tema como argumentos.
    * El PID del proceso recién lanzado se añade al array `PIDS`.
    * Se incluye un comentario sobre una pequeña pausa (`sleep 1`) que está comentada actualmente. Podrías descomentarla si necesitas introducir una pausa entre el lanzamiento de cada productor.
10. Finalmente, se muestra un mensaje indicando que todos los productores han sido iniciados y el script principal entra en un estado de espera (`wait`) hasta que se reciba una señal de interrupción (por ejemplo, al presionar Ctrl+C). Al recibir la señal, se ejecutará la función `cleanup()` para detener todos los productores.

Este script es una herramienta útil para generar carga en tu sistema de message broker con múltiples productores publicando al mismo tema.

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

### Script para Pruebas de Consumers (`test_consumers.sh`)

Este script de Bash está diseñado para facilitar la compilación del programa `consumer.c` y el lanzamiento de múltiples instancias de consumidores para un tema específico, distribuyéndolos cíclicamente entre un conjunto de grupos de consumidores definidos.

#### Funcionalidades

* **Compilación Automática:** Compila el archivo fuente `consumer.c` utilizando `gcc` y la librería `pthread`.
* **Lanzamiento Múltiple de Consumers:** Inicia un número configurable de consumidores.
* **Asignación Cíclica de Grupos:** Asigna cada consumidor a un grupo diferente de forma rotativa, permitiendo probar el comportamiento del balanceo de carga del broker.
* **Especificación de Tema:** Utiliza un tema predefinido para todos los consumidores lanzados.
* **Manejo de Señales:** Implementa un mecanismo de limpieza para detener todos los procesos de consumidor en ejecución al recibir las señales `SIGINT` (Ctrl+C) o `SIGTERM`.
* **Espera de Finalización:** El script principal espera hasta que se reciba una señal de interrupción para finalizar, manteniendo los consumidores en ejecución hasta entonces.

#### Cómo Utilizar

1.  **Guardar el script:** Guarda el código del script en un archivo llamado `test_consumers.sh` (o con el nombre que prefieras).
2.  **Otorgar permisos de ejecución:** Abre una terminal y otorga permisos de ejecución al script:
    ```bash
    chmod +x test_consumers.sh
    ```
3.  **Asegurarse de tener `consumer.c`:** Asegúrate de que el archivo fuente del consumidor (`consumer.c`) se encuentre en el directorio `../src/` relativo a la ubicación del script, tal como se especifica en la línea de compilación.
4.  **Ejecutar el script:** Ejecuta el script desde la terminal:
    ```bash
    ./test_consumers.sh
    ```

#### Configuración

Las siguientes variables en el script pueden ser modificadas para ajustar el comportamiento de las pruebas:

* `TOPIC="noticias"`: Define el tema al que se suscribirán todos los consumidores. Puedes cambiar `"noticias"` por el tema que desees probar.
* `GRUPOS=("g1" "g2" "g3")`: Es un array que contiene los nombres de los grupos de consumidores que se utilizarán. Puedes añadir o eliminar grupos según tus necesidades. Los consumidores se asignarán a estos grupos de forma cíclica.
* `NUM=5`: Define el número total de consumidores que se lanzarán. Puedes ajustar este valor para probar con diferentes cantidades de consumidores.

#### Funcionamiento

1.  El script comienza estableciendo opciones seguras de Bash (`set -euo pipefail`).
2.  Se definen las variables de configuración para el tema, los grupos de consumidores y el número de consumidores a lanzar.
3.  Se inicializa un array vacío (`CONSUMER_PIDS`) para almacenar los IDs de proceso (PIDs) de los consumidores lanzados.
4.  Se define una función `cleanup()` que se ejecutará al recibir las señales `SIGINT` o `SIGTERM`. Esta función itera sobre los PIDs almacenados y envía una señal `kill` a cada proceso de consumidor para detenerlos.
5.  Se establece un `trap` para que la función `cleanup()` se ejecute al recibir las señales mencionadas.
6.  El script intenta compilar el archivo `../src/consumer.c` utilizando `gcc` y enlazando la librería `pthread`. Si la compilación falla, se muestra un error y el script se detiene.
7.  Si la compilación es exitosa, el script procede a lanzar el número especificado de consumidores.
8.  Dentro de un bucle, para cada consumidor:
    * Se calcula un índice para seleccionar un grupo del array `GRUPOS` de forma cíclica.
    * Se obtiene el nombre del grupo correspondiente.
    * Se muestra un mensaje indicando el ID del consumidor y el grupo al que se unirá.
    * Se ejecuta el programa `consumer` en segundo plano (`&`), pasándole el ID del consumidor, el tema y el grupo como argumentos. La entrada estándar del consumidor se redirige desde `/dev/null`.
    * El PID del proceso recién lanzado se añade al array `CONSUMER_PIDS`.
    * Se introduce una breve pausa (`sleep 0.2`) entre el lanzamiento de cada consumidor para evitar una sobrecarga inicial.
9.  Finalmente, se muestra un mensaje indicando que todos los consumidores están activos y el script principal entra en un estado de espera (`wait`) hasta que se reciba una señal de interrupción (por ejemplo, al presionar Ctrl+C). Al recibir la señal, se ejecutará la función `cleanup()` para detener todos los consumidores.

Este script es una herramienta útil para probar y verificar el comportamiento de tu sistema de message broker con múltiples consumidores en diferentes grupos.

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

# Limitaciones del Programa

El sistema de message broker, tal como está implementado, presenta ciertas limitaciones que deben tenerse en cuenta:

* **File Descriptors y Procesos del Sistema Operativo:** Una limitación inherente a cualquier aplicación que maneja conexiones de red es la cantidad de file descriptors y procesos que el sistema operativo puede abrir. Cada conexión de cliente (productor o consumidor) consume un file descriptor. Si el número de clientes simultáneos excede los límites configurados en el sistema operativo, el broker podría fallar al aceptar nuevas conexiones.

    * **Verificación y Ajuste:** En sistemas Linux y similares, estos límites se pueden verificar y, en algunos casos, ajustar utilizando los comandos `ulimit -n` (para el número máximo de file descriptors abiertos por proceso) y `ulimit -u` (para el número máximo de procesos por usuario). Es importante monitorear estos límites en el sistema donde se despliega el broker, especialmente en entornos con un gran número de clientes. Si es necesario soportar una alta concurrencia, se pueden intentar aumentar estos límites, aunque esto requiere privilegios de administrador y debe hacerse con precaución, entendiendo las implicaciones para la estabilidad del sistema.

* **Cantidad de Grupos de Consumidores:** La cantidad de grupos de consumidores que el broker puede gestionar simultáneamente está actualmente limitada por el tamaño del array `group_distribution_states` definido como:

    ```c
    GroupDistributionState group_distribution_states[100];
    int num_group_distribution_states = 0;
    pthread_mutex_t group_distribution_state_mutex = PTHREAD_MUTEX_INITIALIZER;
    ```

    Actualmente, se pueden registrar hasta **100** grupos de consumidores únicos. Si se intenta crear un número mayor de grupos distintos, el comportamiento del broker podría ser impredecible o fallar al registrar el nuevo grupo.

    * **Ajuste:** Esta limitación se puede modificar directamente en el código fuente del broker, cambiando el tamaño del array `group_distribution_states`. Si se anticipa la necesidad de un mayor número de grupos, se deberá aumentar este valor y recompilar el broker. Consideraciones sobre el uso de memoria deben tenerse en cuenta al aumentar este límite significativamente.

* **Tamaño de los Mensajes:** El tamaño máximo del payload de un mensaje está limitado por la constante `MSG_PAYLOAD_SIZE`, definida como **256 bytes**:

    ```c
    #define MSG_PAYLOAD_SIZE 256
    ```

    Los productores no podrán enviar mensajes cuyo payload exceda este tamaño, y el broker y los consumidores esperarán mensajes con un payload dentro de este límite.

* **Longitud de Temas:** La longitud máxima de un tema está limitada por la constante `MAX_TOPIC_LEN` y el tamaño del array `topic` en la estructura `Message`, ambos definidos como **64 bytes**:

    ```c
    #define MAX_TOPIC_LEN 64

    typedef struct {
        // ...
        char topic[64];
        // ...
    } Message;
    ```

    Los productores y consumidores no podrán utilizar temas que excedan esta longitud.

* **Capacidad de las Colas de Mensajes:** La capacidad de la cola principal de mensajes del broker (`msg_queue`) y la cola de logs (`log_queue`) están limitadas por la constante `MSG_QUEUE_CAPACITY` y `LOG_QUEUE_CAPACITY`, respectivamente, ambos definidos como **100 mensajes**:

    ```c
    #define LOG_QUEUE_CAPACITY 100
    #define MSG_QUEUE_CAPACITY 100

    // ...

    typedef struct {
        // ...
        Message messages[MSG_QUEUE_CAPACITY];
    } MessageQueue;

    typedef struct {
        // ...
        LogMessage messages[LOG_QUEUE_CAPACITY];
    } LogQueue;
    ```

    Si la tasa de producción de mensajes excede la capacidad de procesamiento del broker o la tasa de escritura de logs, estas colas podrían llenarse, lo que podría resultar en la pérdida de mensajes (en el caso de la cola principal) o la pérdida de logs.

* **Historial de Mensajes:** El broker mantiene un historial de los últimos `MAX_MESSAGE_HISTORY` (**1000**) mensajes:

    ```c
    #define MAX_MESSAGE_HISTORY 1000
    Message message_history[MAX_MESSAGE_HISTORY];
    int message_history_count = 0;
    pthread_mutex_t message_history_mutex = PTHREAD_MUTEX_INITIALIZER;
    ```

    Los consumidores que se conecten y soliciten mensajes desde el inicio solo podrán acceder a los últimos 1000 mensajes almacenados en este historial. Los mensajes más antiguos se perderán del historial.

* **Tamaño de la Cola de Tareas del Thread Pool:** El thread pool utilizado por el broker tiene una cola de tareas implícita limitada por la velocidad a la que los threads pueden procesar las tareas entrantes. Si la tasa de llegada de nuevas conexiones o mensajes excede la capacidad del thread pool (actualmente con un tamaño de **50 threads** definido por `THREAD_POOL_SIZE`), las tareas podrían acumularse, lo que podría llevar a una latencia mayor en el procesamiento.

* **Cantidad de Temas por Suscripción:** Cada consumidor, en la estructura `Subscription`, tiene espacio para suscribirse a un máximo de **10** temas:

    ```c
    typedef struct Subscription {
        // ...
        char topics[10][64];
        int topic_count;
        // ...
    } ConsumerSubscription;
    ```

    Un consumidor no podrá suscribirse a más de 10 temas simultáneamente.

* **Miembros por Grupo:** La implementación de grupos de consumidores podría tener limitaciones implícitas en la forma en que se gestionan los miembros dentro de un grupo, aunque no se define una constante explícita para el número máximo de miembros por grupo en las estructuras proporcionadas. Sin embargo, la lógica de distribución dentro de los grupos podría tener consideraciones de eficiencia con un número muy grande de miembros.

Es importante tener en cuenta estas limitaciones al diseñar la arquitectura y el despliegue del sistema de message broker, y considerar si es necesario ajustar estas limitaciones en función de los requisitos específicos de la aplicación.