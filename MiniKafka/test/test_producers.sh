#!/usr/bin/env bash

# =========================================================
# Script para compilar y lanzar 5 producers con IDs del 1 al 5
# =========================================================

set -euo pipefail

# Compilar el ejecutable del producer
echo "Compilando producer..."
gcc -o producer ../src/producer.c -lpthread
if [ $? -ne 0 ]; then
  echo "Error compilando producer"
  exit 1
fi

echo "Compilación del producer completada."

# Verificar argumentos
if [ $# -ne 1 ]; then
  echo "Uso: $0 <topic>"
  echo "  <topic> : Topic para todos los producers"
  exit 1
fi

# Topic compartido para todos los producers
TOPIC=$1

# Array para guardar PIDs de los producers lanzados
PIDS=()

# Función para manejar SIGINT/SIGTERM y limpiar producers
cleanup() {
  echo
  echo "Deteniendo producers..."
  for pid in "${PIDS[@]}"; do
    kill "$pid" 2>/dev/null || true
  done
  exit 0
}

trap cleanup SIGINT SIGTERM

# Lanzar producers con IDs del 1 al 5
echo "Iniciando 5 producers para el topic '${TOPIC}'..."
clear
for PRODUCER_ID in {1..100}; do
  echo "  - Producer ID=${PRODUCER_ID}"
  ./producer "$PRODUCER_ID" "$TOPIC" &
  PIDS+=("$!")
  # Pequeña pausa para evitar choque inicial de conexiones
  # sleep 1
done

echo "Todos los producers han sido iniciados. Presiona Ctrl+C para detenerlos."

# Esperar señal o al fin de los procesos
wait

echo "Todos los producers han finalizado."
