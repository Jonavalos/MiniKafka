#include <stdio.h>
#include <string.h>

#define MAX_MESSAGES 10

typedef struct {
   int id;
   char content[256];
} Message;

typedef struct {
   Message messages[MAX_MESSAGES];
   int head;
   int tail;
   int count;
} MessageQueue;

void init_queue(MessageQueue* queue) {
   queue->head = 0;
   queue->tail = 0;
   queue->count = 0;
}

int enqueue(MessageQueue* queue, Message msg) {
   if (queue->count == MAX_MESSAGES) return -1;
   queue->messages[queue->tail] = msg;
   queue->tail = (queue->tail + 1) % MAX_MESSAGES;
   queue->count++;
   return 0;
}

int dequeue(MessageQueue* queue, Message* msg) {
   if (queue->count == 0) return -1;
   *msg = queue->messages[queue->head];
   queue->head = (queue->head + 1) % MAX_MESSAGES;
   queue->count--;
   return 0;
}

int main() {
   MessageQueue queue;
   Message msg;

   init_queue(&queue);

   Message new_msg = { 1, "Hello world" };
   enqueue(&queue, new_msg);

   if (dequeue(&queue, &msg) == 0) {
       printf("Message: %d - %s\n", msg.id, msg.content);
   }

   return 0;
}
