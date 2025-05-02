CC = gcc
CFLAGS = -Wall -Wextra -pthread
LDFLAGS = -pthread -lrt
SRC_DIR = src
BIN_DIR = .

all: $(BIN_DIR)/broker $(BIN_DIR)/producer $(BIN_DIR)/consumer

$(BIN_DIR)/broker: $(SRC_DIR)/broker.c
	$(CC) $(CFLAGS) -o $@ $< $(LDFLAGS)

$(BIN_DIR)/producer: $(SRC_DIR)/producer.c
	$(CC) $(CFLAGS) -o $@ $< $(LDFLAGS)

$(BIN_DIR)/consumer: $(SRC_DIR)/consumer.c
	$(CC) $(CFLAGS) -o $@ $< $(LDFLAGS)

clean:
	rm -f $(BIN_DIR)/broker $(BIN_DIR)/producer $(BIN_DIR)/consumer

.PHONY: all clean