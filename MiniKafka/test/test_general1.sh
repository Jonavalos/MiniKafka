#!/usr/bin/env bash

# =========================================================
# Script para compilar y ejecutar el broker, consumer y producer
# =========================================================

# Compilar los ejecutables
echo "Compilando broker, consumer y producer..."
gcc -o broker ../src/broker.c -lpthread
if [ $? -ne 0 ]; then
  echo "Error compilando broker"
  exit 1
fi
# gcc -o consumer ../src/consumer.c -lpthread
# if [ $? -ne 0 ]; then
#   echo "Error compilando consumer"
#   exit 1
# fi
gcc -o producer ../src/producer.c -lpthread
if [ $? -ne 0 ]; then
  echo "Error compilando producer"
  exit 1
fi

echo "Compilación completada."

# Verificar argumentos
if [ $# -ne 4 ]; then
  echo "Uso: $0 <consumer_id> <topic> <group_id> <producer_id>"
  echo "  <consumer_id>  : ID numérico para el consumer"
  echo "  <topic>        : Topic compartido (mismo para consumer y producer)"
  echo "  <group_id>     : Nombre del grupo (cualquier cadena)"
  echo "  <producer_id>  : ID numérico para el producer"
  exit 1
fi

CONSUMER_ID=$1
TOPIC=$2
GROUP_ID=$3
PRODUCER_ID=$4

# Lanzar broker en segundo plano
echo "Iniciando broker..."
../src/broker &
BROKER_PID=$!

# Pequeña pausa para asegurar que el broker esté listo
sleep 1

# Lanzar consumer en segundo plano
echo "Iniciando consumer (ID=${CONSUMER_ID}, topic=${TOPIC}, grupo=${GROUP_ID})..."
../src/consumer ${CONSUMER_ID} ${TOPIC} ${GROUP_ID} &
CONSUMER_PID=$!

# Pequeña pausa para estabilizar
sleep 1

# Lanzar producer en primer plano
echo "Iniciando producer (ID=${PRODUCER_ID}, topic=${TOPIC})..."
../src/producer ${PRODUCER_ID} ${TOPIC}

# Cuando el producer termine, cerramos todo
echo "Producer finalizó, deteniendo broker y consumer..."
kill ${BROKER_PID}
kill ${CONSUMER_PID}

echo "Todos los procesos detenidos. Fin del script."