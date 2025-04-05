#include <stdio.h>
#include <stdlib.h>
#include <pthread.h>
#include <unistd.h>  // para sleep

pthread_mutex_t lock;

void* thread_func(void* arg) {
    int id = *(int*)arg;

    for (int i = 0; i < 5; i++) {
        // Intentar adquirir el mutex sin bloquear
        if (pthread_mutex_trylock(&lock) == 0) {
            printf("Hilo %d accede a la sección crítica (iteración %d)\n", id, i);
            sleep(1); // Simula trabajo en la sección crítica
            pthread_mutex_unlock(&lock);
        }
        else {
            printf("Hilo %d NO pudo acceder a la sección crítica (iteración %d)\n", id, i);
        }

        sleep(1); // Espera antes de volver a intentar
    }

    return NULL;
}

int main() {
    pthread_t threads[3];
    int ids[3] = { 1, 2, 3 };

    // Inicializar mutex
    pthread_mutex_init(&lock, NULL);

    // Crear hilos
    for (int i = 0; i < 3; i++) {
        pthread_create(&threads[i], NULL, thread_func, &ids[i]);
    }

    // Esperar a que los hilos terminen
    for (int i = 0; i < 3; i++) {
        pthread_join(threads[i], NULL);
    }

    // Destruir mutex
    pthread_mutex_destroy(&lock);

    return 0;
}
