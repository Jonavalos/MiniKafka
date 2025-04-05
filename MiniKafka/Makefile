CC = gcc
CFLAGS = -Wall -pthread

# Objetivo por defecto
all: queue_example

# Regla para compilar el ejecutable
queue_example: mutexEG.c
	$(CC) $(CFLAGS) mutexEG.c -o queue_example

# Regla para limpiar los archivos generados
clean:
	rm -f queue_example
