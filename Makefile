CC = gcc
CFLAGS = -Wall -Wextra -O2
LDLIBS = -lpthread

all: main_server main_client

main_server: main_server.c net_common.h
	$(CC) $(CFLAGS) -o $@ main_server.c $(LDLIBS)

main_client: main_client.c net_common.h
	$(CC) $(CFLAGS) -o $@ main_client.c $(LDLIBS)

clean:
	rm -f main_client main_server

.PHONY: all clean
