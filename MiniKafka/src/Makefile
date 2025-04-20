CC = gcc
CFLAGS = -Wall -Wextra -pthread
LDFLAGS = -pthread -lrt

all: broker producer consumer

broker: broker.c
	$(CC) $(CFLAGS) -o broker broker.c $(LDFLAGS)

producer: producer.c
	$(CC) $(CFLAGS) -o producer producer.c $(LDFLAGS)

consumer: consumer.c
	$(CC) $(CFLAGS) -o consumer consumer.c $(LDFLAGS)

clean:
	rm -f broker producer consumer *.o

.PHONY: all clean