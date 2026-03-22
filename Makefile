CC      ?= gcc
AR      ?= ar
CFLAGS  := -Wall -Wextra -std=c11 -D_GNU_SOURCE -Os
CFLAGS  += -Iinclude
SRCS    := src/rss_ring.c src/rss_osd_shm.c src/rss_ctrl.c
OBJS    := $(SRCS:.c=.o)
LIB     := librss_ipc.a

all: $(LIB)

$(LIB): $(OBJS)
	$(AR) rcs $@ $^

%.o: %.c
	$(CC) $(CFLAGS) -c $< -o $@

clean:
	rm -f $(OBJS) $(LIB)

.PHONY: all clean
