#!/usr/bin/env bash

set -euo pipefail

# 1) Compilar
echo "Compilando broker, consumer y producer..."
gcc -o broker ../src/broker.c -lpthread && echo "Broker OK" || { echo "Error broker"; exit 1; }
gcc -o consumer ../src/consumer.c -lpthread && echo "Consumer OK" || { echo "Error consumer"; exit 1; }
gcc -o producer ../src/producer.c -lpthread && echo "Producer OK" || { echo "Error producer"; exit 1; }

# 2) Parámetros fijos
TOPIC="noticias"
GRUPOS=("g1" "g2" "g3")

# Depuración - ver qué contiene GRUPOS
echo "DEBUG: Contenido de GRUPOS:"
for ((i=0; i<${#GRUPOS[@]}; i++)); do
  echo "  GRUPOS[$i] = '${GRUPOS[$i]}'"
done

NUM=5               # número de consumers y producers

# 3) Arreglo de PIDs
PIDS=()

cleanup() {
  echo
  echo "Deteniendo todo..."
  for pid in "${PIDS[@]}"; do kill "$pid" &>/dev/null || true; done
  exit 0
}
trap cleanup SIGINT SIGTERM

# 4) Lanzar broker
echo "Iniciando broker..."
./broker &
PIDS+=("$!")
sleep 1

# Mensaje modificado para evitar problemas de expansión
echo "Iniciando $NUM consumers para topic='$TOPIC', grupos: g1, g2, g3..."

# 5) Lanzar consumers
for ((i=1; i<=NUM; i++)); do
  idx=$(( (i-1) % ${#GRUPOS[@]} ))
  grp="${GRUPOS[$idx]}"
  echo "  → Consumer ID=$i, Grupo=$grp"
  ./consumer "$i" "$TOPIC" "$grp" &
  PIDS+=("$!")
  sleep 0.2
done

# 6) Lanzar producers
echo "Iniciando $NUM producers para topic='$TOPIC'..."
for ((i=1; i<=NUM; i++)); do
  echo "  → Producer ID=$i"
  ./producer "$i" "$TOPIC" &
  PIDS+=("$!")
  sleep 0.2
done

echo
echo "Todo lanzado. Presiona Ctrl+C para parar."
wait