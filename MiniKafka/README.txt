
===================================== 📦 Estructuras ===============================

Message
Estructura que representa un mensaje producido por un cliente. Contiene un identificador único (id), el identificador del productor (producer_id), el nombre del tópico (topic) y el contenido del mensaje (payload). Está alineada con #pragma pack(1) para evitar relleno de memoria.

message_history
Arreglo estático que almacena el historial de mensajes enviados, hasta un máximo definido por MAX_MESSAGE_HISTORY. Protegido por un mutex (message_history_mutex) para garantizar acceso concurrente seguro.

MessageQueue
Cola circular de mensajes compartida entre productores y consumidores. Implementada con sincronización basada en mutex (mutex), condiciones (not_empty, not_full) y semáforos (empty_slots, filled_slots). Contiene una bandera de apagado (shutdown) y un contador de IDs (next_message_id) para asignar identificadores únicos a los mensajes.

LogMessage
Estructura utilizada para representar un mensaje de log. Contiene una marca de tiempo (timestamp) y el contenido textual del mensaje (message), con longitud máxima definida por LOG_MSG_SIZE.

LogQueue
Cola circular utilizada exclusivamente para el sistema de logging asincrónico. Sincronizada mediante mutex y variables de condición. Es consumida por un hilo dedicado (logger_thread) y soporta apagado controlado mediante la bandera shutdown.

ConsumerSubscription
Nodo de una lista enlazada que representa a un consumidor suscrito. Contiene el identificador del consumidor (consumer_id), descriptor de socket (socket_fd), una lista de tópicos suscritos (topics) y la cantidad total de suscripciones (topic_count). Protegido por subscription_mutex.

ConsumerOffset
Estructura que lleva el control del último offset consumido por un consumidor específico en un grupo y tópico determinado. Utilizada para mantener consistencia en el consumo por grupo. Forma parte de una lista enlazada protegida por consumer_offsets_mutex.

ProdArg
Estructura auxiliar utilizada para pasar argumentos a hilos de manejo de productores. Contiene el descriptor de archivo (fd) y el identificador del productor (producer_id).

GroupDistributionState
Estado de distribución de un grupo de consumidores para un tópico específico. Utilizado para realizar balanceo tipo round-robin en el envío de mensajes. Protegido mediante group_distribution_state_mutex.

GroupMember
Nodo de una lista enlazada que representa un miembro de un grupo de consumidores. Contiene información del tópico y grupo al que pertenece, el identificador del consumidor y su socket. Protegido por group_members_mutex.

TaskType
Enumeración que define los tipos de tarea manejadas por el thread pool: suscripción (TASK_SUBSCRIBE), desuscripción (TASK_UNSUBSCRIBE) y producción (TASK_PRODUCE).

TaskData
Estructura que encapsula los datos asociados a una tarea. Incluye el tipo (type), el descriptor del cliente (client_fd), identificador del consumidor (consumer_id), tópico (topic), identificador de grupo (group_id) y el offset inicial desde el cual consumir (start_offset).

TaskNode
Nodo de una lista enlazada que contiene una instancia de TaskData. Utilizado para implementar la cola de tareas del thread pool.

TaskQueue
Cola sincronizada de tareas pendientes para el thread pool. Utiliza un mutex (mutex) y una variable de condición (not_empty) para coordinar el acceso entre múltiples hilos. Soporta apagado limpio mediante la bandera shutdown.

ThreadPool
Contenedor del thread pool, que incluye un arreglo de hilos (threads) y una instancia de TaskQueue. Se encarga de gestionar las tareas concurrentes del broker sin bloquear la ejecución principal.




===================================== 🧩 Metodos ===============================

//******************************** 🧵 T H R E A D    P O O L ***************************** */

signal_handler(int signum)
Manejador de señales que permite la terminación controlada del broker. Cambia la bandera global running a 0, guarda los offsets de consumidores activos mediante save_consumer_offsets() y señala al hilo de logging para que finalice correctamente.

init_task_queue(TaskQueue *q)
Inicializa una instancia de TaskQueue, configurando los punteros de cabeza y cola, la bandera de apagado (shutdown), el mutex y la variable de condición necesaria para la sincronización de acceso a la cola.

enqueue_task_data(TaskQueue *q, TaskData *td)
Inserta una nueva tarea en la cola de tareas. Crea dinámicamente un nodo TaskNode, lo enlaza al final de la cola y señala a los hilos trabajadores mediante pthread_cond_signal para que procesen la nueva tarea.

dequeue_task(TaskQueue *q)
Extrae una tarea de la cola de tareas. Si la cola está vacía, espera con pthread_cond_wait. Retorna NULL si la cola está en modo apagado (shutdown). Garantiza acceso seguro usando mutex.

worker_loop(void *arg)
Función principal ejecutada por los hilos del thread pool. Obtiene tareas de la cola y ejecuta su lógica según el tipo (TASK_SUBSCRIBE, TASK_UNSUBSCRIBE, etc.). Libera los nodos procesados y finaliza cuando dequeue_task devuelve NULL.

init_thread_pool(ThreadPool *p)
Inicializa el thread pool, configurando la cola de tareas y lanzando THREAD_POOL_SIZE hilos que ejecutan la función worker_loop.

shutdown_thread_pool(ThreadPool *p)
Detiene ordenadamente todos los hilos del thread pool. Marca la cola como cerrada (shutdown = true), despierta a todos los hilos esperando tareas y espera a que terminen con pthread_join. Luego, libera recursos sincronizados (mutex y condición).

//*************************** F I N    T H R E A D    P O O L *************************** */



//************************************ 🍟 G R O U P I N G ********************************** */

add_group_member(const char *topic, const char *group_id, int consumer_id, int socket_fd)
Agrega un consumidor a un grupo de consumidores asociado a un tópico específico. Inserta un nuevo nodo GroupMember en la lista enlazada group_members, protegiendo el acceso concurrente con group_members_mutex.

remove_group_member(const char *topic, const char *group_id, int consumer_id)
Elimina a un consumidor específico de un grupo de un tópico dado. Si el grupo queda vacío tras la eliminación, limpia el estado de distribución (GroupDistributionState) asociado al grupo. Protege la operación con group_members_mutex.

remove_group_distribution_state(const char *topic, const char *group_id)
Elimina el estado de distribución de mensajes (GroupDistributionState) para un grupo dado. Esto asegura que ya no se realicen rondas de distribución para ese grupo en particular. El acceso está sincronizado con group_distribution_state_mutex.

handle_consumer_disconnect(int consumer_id)
Gestiona la desconexión lógica de un consumidor, eliminándolo de todos los grupos/tópicos en los que esté registrado. Esta operación reutiliza remove_group_member y remove_subscription, garantizando una limpieza completa del estado del consumidor.

handle_socket_disconnect(int socket_fd)
Identifica el consumer_id asociado a un socket_fd y delega el manejo completo de desconexión a handle_consumer_disconnect. También cierra el socket al finalizar. Esta función asegura la liberación ordenada de recursos ante desconexiones abruptas.


//************************************  F I N	 G R O U P I N G ***************************** */



//*********************************** 📡 S U B S C R I P T I O N S ****************************** */

process_subscription_request(int consumer_id, int socket_fd, const char *topic, const char *group_id, long long start_offset)
Procesa la solicitud inicial de suscripción de un consumidor. Establece el offset de inicio: si start_offset == -1, se utiliza el offset más reciente disponible. Luego, registra al consumidor en el grupo correspondiente mediante add_group_member.

add_subscription(int consumer_id, int socket_fd, const char *topic, const char *group_id)
Agrega un tópico a la lista de suscripciones activas de un consumidor. Si no existe una suscripción previa, se crea una nueva entrada ConsumerSubscription. También se asegura que el consumidor esté registrado en el grupo con add_group_member y se actualiza su offset al siguiente mensaje disponible (last_offset + 1). Sincronizado mediante subscription_mutex.

remove_subscription(int consumer_id, const char *topic)
Elimina un tópico específico de la lista de suscripciones del consumidor. Si, tras la eliminación, el consumidor ya no tiene suscripciones activas, se libera completamente su estructura ConsumerSubscription. Protegido por subscription_mutex para evitar condiciones de carrera.

//*********************************** F I N	 S U B S C R I P T I O N S ****************************** */



//***************************************** 📨 M E S S A G I N G **************************************** */

static int find_consumer_id_by_socket(int sock, const char *topic, const char *group_id)
Busca y retorna el consumer_id asociado a un socket específico dentro de un grupo y tópico dados, protegiendo el acceso con mutex.

int get_next_consumer_in_group(const char *topic, const char *group_id)
Selecciona el socket del próximo consumidor en un grupo de consumidores usando round-robin, manteniendo estado entre llamadas y asegurando sincronización mediante mutex.

void *message_processor_thread(void *arg)
Hilo que procesa mensajes de la cola, determina a qué grupo pertenecen y delega la distribución al método correspondiente.

void distribute_message(const char *topic, const char *message)
Distribuye un mensaje a todos los grupos suscritos a un tópico, seleccionando consumidores disponibles mediante round-robin y reintentando envíos fallidos hasta un máximo predefinido.

int init_msg_queue()
Inicializa la cola de mensajes compartida en memoria usando shm_open, semáforos POSIX y un mutex compartido entre procesos.

void enqueue_message(int producer_id, const char *topic, const char *payload)
Encola un nuevo mensaje en la cola circular compartida, sincronizando el acceso con semáforos y mutex.

bool dequeue_message(Message *out_msg)
Extrae un mensaje de la cola compartida, sincronizando el acceso con semáforos y mutex, y lo guarda en el historial para trazabilidad.

void cleanup_msg_queue()
Libera correctamente los recursos del sistema asociados a la cola de mensajes en memoria compartida, destruyendo los mutex y variables de condición, desmapeando la memoria y desvinculando el objeto shm.

void *connection_handler_thread(void *arg)
Función principal de escucha para nuevas conexiones de clientes. Acepta conexiones, determina si son consumidores o productores mediante "peek" o handshake binario, y lanza el procesamiento correspondiente, ya sea agregando una tarea a la cola o creando un hilo para productores.

static ssize_t read_n_bytes(int fd, void *buf, size_t count)
Lee exactamente count bytes desde un descriptor de archivo fd al búfer buf, manejando casos donde read() retorna menos de lo solicitado. Devuelve el total de bytes leídos o error.

void *producer_handler_thread_fd(void *arg)
Hilo dedicado al manejo de un productor. Lee estructuras Message completas del socket, valida sus campos y las encola para su procesamiento, desconectando al productor en caso de errores o finalización.

//************************************** F I N	 M E S S A G I N G ************************************ */



//****************************************** 🖋️ L O G G I N G ****************************************** */

void *log_writer_thread(void *arg)
Hilo dedicado a escribir mensajes de log desde la cola en memoria compartida hacia el archivo broker.log, manejando sincronización con mutex y condiciones hasta que el sistema se apague y la cola esté vacía.

int init_log_queue()
Inicializa la cola de logs en memoria compartida (shm), configurando sus atributos de sincronización compartidos entre procesos, e inicializando su estado interno (punteros de cabeza, cola y contador).

void enqueue_log(const char *format, ...)
Agrega un nuevo mensaje formateado a la cola de logs con timestamp actual, esperando si la cola está llena y señalando a los consumidores una vez insertado, usando sincronización con mutex y variables de condición.

void shutdown_logger()
Solicita un apagado ordenado del sistema de logging, activando la bandera de cierre (shutdown), notificando al hilo escritor y esperando a que termine antes de liberar los recursos.

void cleanup_log_queue()
Libera los recursos del sistema asociados a la cola de logs: destruye los mutex y condiciones, desmapea la memoria y elimina el segmento compartido (shm_unlink).


//*************************************** F I N		L O G G I N G ******************************* */



//********************************************🪧 O F F S E T S *************************************** */

void update_consumer_offset(const char *topic, const char *group_id, int consumer_id, long long message_id)
Actualiza o crea el registro del último offset consumido por un consumidor específico dentro de un grupo y tópico, usando una lista enlazada protegida con mutex.

long long get_last_consumer_offset(const char *topic, const char *group_id, int consumer_id)
Retorna el último offset consumido por un consumidor específico en un grupo y tópico, o -1 si no existe registro.

long long get_group_last_offset(const char *topic, const char *group_id)
Obtiene el offset más alto consumido por cualquier consumidor de un grupo específico en un tópico determinado.

void save_consumer_offsets()
Guarda en el archivo consumer_offsets.dat todos los offsets consumidos registrados en la lista enlazada, para persistencia entre ejecuciones.

void load_consumer_offsets()
Carga desde el archivo consumer_offsets.dat los offsets guardados, reconstruyendo la lista enlazada en memoria.

void send_messages_from_offset(int consumer_id, int client_fd, const char *topic, const char *group_id, long long start_offset)
Envía al consumidor los mensajes históricos de un tópico a partir de un offset específico, recorriendo el buffer circular en memoria.

void store_message_in_history(const Message *msg)
Guarda un mensaje en el historial de mensajes en memoria (con buffer circular), eliminando el más antiguo si se alcanza la capacidad máxima.

//**************************************** F I N	O F F S E T S ******************************* */


=============================================================== 🐯 M A I N ================================================================
Manejo de señales para un apagado limpio
	-Configura el manejo de señales (SIGINT, SIGTERM) para permitir una terminación ordenada del proceso, asegurando que todos los recursos se liberen correctamente.

Inicialización de la cola de logs
	-Llama a init_log_queue() para configurar la cola de logs que se utilizará en el hilo de registro. Si falla, se imprime un mensaje de error y el programa termina.

Inicialización de la cola de mensajes
	-Llama a init_msg_queue() para configurar la cola que manejará los mensajes entre los productores y consumidores. Si falla, se imprime un error, se limpian los recursos de logs, y 	el programa termina.

Carga de los offsets de consumidores previos
	-Llama a load_consumer_offsets() para cargar los offsets de los consumidores desde un archivo persistido previamente. Esto permite que el broker sepa desde qué mensaje debe empezar 	a enviar a cada consumidor.

Inicialización del grupo de hilos (thread pool)
	-Se inicializa un pool de hilos para manejar las operaciones de los consumidores (SUBSCRIBE, UNSUBSCRIBE, PRODUCE). Este paso permite que múltiples operaciones se manejen 	simultáneamente.

Creación del hilo de escritura de logs
	-Se crea un hilo que ejecuta la función log_writer_thread, encargado de escribir los mensajes de log en un archivo. Si no puede crearse el hilo, se limpian los recursos y el 	programa termina.

Creación del hilo para el procesamiento de mensajes
	-Se crea un hilo que ejecuta la función message_processor_thread, encargado de procesar los mensajes entrantes de los productores y enviarlos a los consumidores. Si no puede 	crearse el hilo, se realiza el apagado ordenado de los recursos y el programa termina.

Configuración del socket del broker
	-Se configura un socket TCP para el broker que escuchará conexiones entrantes de los clientes (productores y consumidores).
	-Se configuran opciones como SO_REUSEADDR y SO_REUSEPORT para permitir la reutilización de direcciones y puertos.
	-Se vincula el socket al puerto 8080 y se pone a la espera de conexiones.

Creación del hilo de aceptación de conexiones
	-Se crea un hilo que ejecutará la función connection_handler_thread, encargada de aceptar nuevas conexiones y procesarlas. Si no se puede crear el hilo, se cierran los recursos del 	servidor y el programa entra en la sección de limpieza.

Esperar la señal de apagado
	-El programa entra en un bucle de espera, manteniéndose activo hasta recibir una señal de apagado (SIGINT o SIGTERM).

Apagado ordenado
	-En caso de recibir la señal de apagado, se realiza un apagado ordenado de todos los recursos:
		Se cierra el socket de aceptación de conexiones.
		Se apaga el thread pool, liberando los hilos de trabajo.
		Se apaga el hilo de escritura de logs.
		Se señala al procesador de mensajes que debe detenerse, notificando a través de la cola de mensajes.
		Se esperan a que todos los hilos terminen mediante pthread_join.

Finalmente, se limpian las colas de mensajes y logs.

Manejo de errores
	-Si alguna de las operaciones de inicialización falla, se procede a limpiar los recursos previamente asignados antes de salir del programa con un código de error. En caso de éxito, 	se termina correctamente con un valor de retorno 0.

Este flujo garantiza que el broker maneje de manera eficiente y segura la comunicación con los productores y consumidores, manteniendo un manejo robusto de los recursos y permitiendo una desconexión ordenada cuando sea necesario.

