CC      ?= gcc
CFLAGS  := -Wall -Wextra -std=c11 -D_GNU_SOURCE -Os -fPIC
CFLAGS  += -ffunction-sections -fdata-sections
CFLAGS  += -fno-asynchronous-unwind-tables -fmerge-all-constants -fno-ident
CFLAGS  += -Iinclude
SRCS    := src/rss_ring.c src/rss_osd_shm.c src/rss_ctrl.c
OBJS    := $(SRCS:.c=.o)
LIB     := librss_ipc.so

all: $(LIB)

$(LIB): $(OBJS)
	$(CC) -shared -Wl,-soname,librss_ipc.so -o $@ $^

%.o: %.c
	$(CC) $(CFLAGS) -c $< -o $@

clean:
	rm -f $(OBJS) $(LIB)

.PHONY: all clean
