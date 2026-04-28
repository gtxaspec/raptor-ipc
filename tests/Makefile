CC ?= gcc
CFLAGS = -Wall -Wextra -std=c11 -D_GNU_SOURCE -g -fsanitize=address -I../include -I.
LDFLAGS = -fsanitize=address -lrt -lpthread

TEST_SRCS = main.c test_ring.c test_osd.c test_ctrl.c
LIB_SRCS = ../src/rss_ring.c ../src/rss_osd_shm.c ../src/rss_ctrl.c ../src/rss_ipc_log.c

tests: $(TEST_SRCS) $(LIB_SRCS) greatest.h
	$(CC) $(CFLAGS) -o $@ $(TEST_SRCS) $(LIB_SRCS) $(LDFLAGS)

test: tests
	./tests

clean:
	rm -f tests

.PHONY: test clean
