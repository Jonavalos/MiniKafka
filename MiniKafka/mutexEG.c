#include <stdio.h>
#include <stdlib.h>
#include <pthread.h>
#include <unistd.h>

// Nodo de la cola
typedef struct Node {
    int data;
    struct Node* next;
} Node;

// Cola con mutex
typedef struct Queue {
    Node* front;
    Node* rear;
    pthread_mutex_t mutex;
} Queue;

// Inicializa la cola
void initQueue(Queue* q) {
    q->front = q->rear = NULL;
    pthread_mutex_init(&q->mutex, NULL);
}

// Enqueue (agregar elemento al final)
void enqueue(Queue* q, int value) {
    pthread_mutex_lock(&q->mutex);

    Node* newNode = malloc(sizeof(Node));
    newNode->data = value;
    newNode->next = NULL;

    if (q->rear == NULL) {
        q->front = q->rear = newNode;
    } else {
        q->rear->next = newNode;
        q->rear = newNode;
    }

    printf("Enqueued: %d\n", value);
    pthread_mutex_unlock(&q->mutex);
}

// Dequeue (sacar elemento del frente)
int dequeue(Queue* q, int* value) {
    pthread_mutex_lock(&q->mutex);

    if (q->front == NULL) {
        pthread_mutex_unlock(&q->mutex);
        return 0; // cola vacía
    }

    Node* temp = q->front;
    *value = temp->data;
    q->front = q->front->next;

    if (q->front == NULL)
        q->rear = NULL;

    free(temp);
    pthread_mutex_unlock(&q->mutex);
    return 1;
}

// Liberar la cola
void destroyQueue(Queue* q) {
    int val;
    while (dequeue(q, &val));
    pthread_mutex_destroy(&q->mutex);
}


#define NUM_OPS 5

Queue q;

void* producer(void* arg) {
    for (int i = 0; i < NUM_OPS; i++) {
        enqueue(&q, i + 1);
        sleep(1);
    }
    return NULL;
}

void* consumer(void* arg) {
    int val;
    for (int i = 0; i < NUM_OPS; i++) {
        while (!dequeue(&q, &val)) {
            // esperar si está vacía
            usleep(100000);
        }
        printf("Dequeued: %d\n", val);
        sleep(1);
    }
    return NULL;
}

int main() {
    initQueue(&q);

    pthread_t prod, cons;
    pthread_create(&prod, NULL, producer, NULL);
    pthread_create(&cons, NULL, consumer, NULL);

    pthread_join(prod, NULL);
    pthread_join(cons, NULL);

    destroyQueue(&q);
    return 0;
}

