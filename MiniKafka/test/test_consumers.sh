#!/usr/bin/env bash

set -euo pipefail

# =========================================================
# Script para compilar consumer.c y lanzar múltiples consumers
# =========================================================

# Parámetros fijos
TOPIC="noticias"
GRUPOS=("g1" "g2" "g3")
NUM=5  # número de consumers

# Arreglo de PIDs
CONSUMER_PIDS=()

# Función de limpieza
cleanup() {
  echo -e "\nDeteniendo consumers..."
  for pid in "${CONSUMER_PIDS[@]}"; do
    kill "$pid" 2>/dev/null || true
  done
  exit 0
}
trap cleanup SIGINT SIGTERM

# Compilar el consumer
echo "Compilando consumer.c..."
gcc -o consumer ../src/consumer.c -lpthread
if [ $? -ne 0 ]; then
  echo "Error compilando consumer.c"
  exit 1
fi
echo "Compilación exitosa."

# Lanzar consumers
echo -e "\nIniciando $NUM consumers para topic='$TOPIC', grupos: ${GRUPOS[*]}..."
for ((i=1; i<=NUM; i++)); do
  # Asignar grupo cíclicamente (g1, g2, g3, g1...)
  idx=$(( (i-1) % ${#GRUPOS[@]} ))
  grp="${GRUPOS[$idx]}"
  
  echo "  → Consumer ID=$i, Grupo=$grp"
./consumer "$i" "$TOPIC" "$grp" < /dev/null &
  CONSUMER_PIDS+=("$!")
  sleep 0.2  # Pausa breve entre lanzamientos
done

echo -e "\nTodos los consumers ($NUM) están activos. Presiona Ctrl+C para detenerlos."
wait  # Espera hasta recibir SIGINT/SIGTERM