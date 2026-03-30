/*
 * rss_osd_shm.c -- OSD double-buffered SHM implementation.
 *
 * Double-buffered BGRA bitmap transport for OSD overlays.
 * ROD renders into the inactive buffer, atomically swaps, and
 * signals RVD via eventfd. RVD reads the active buffer and
 * pushes it to the HAL OSD region.
 *
 * Memory layout (single contiguous mmap):
 *
 *   +-------------------+  offset 0
 *   | rss_osd_header_t  |  control block
 *   +-------------------+  offset PAGE_SIZE (page-aligned)
 *   | buf[0]            |  BGRA bitmap (stride * height bytes)
 *   +-------------------+
 *   | buf[1]            |  BGRA bitmap (stride * height bytes)
 *   +-------------------+
 */

#include "rss_ipc.h"

#include <errno.h>
#include <fcntl.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/eventfd.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>

#ifndef PAGE_SIZE
#define PAGE_SIZE 4096
#endif

#define RSS_OSD_MAGIC 0x52534F44 /* "RSOD" */
#define RSS_OSD_VERSION 1

#define RSS_OSD_SHM_PREFIX "/rss_osd_"

typedef struct {
    uint32_t width;
    uint32_t height;
    uint32_t stride;        /* bytes per row (width * 4 for BGRA) */
    uint32_t buf_size;      /* single buffer size = stride * height */
    _Atomic int active_buf; /* 0 or 1: which buffer consumer reads */
    _Atomic int dirty;      /* 1 = active_buf has new data */
    uint32_t magic;
    uint32_t version;
    uint32_t _reserved;
} rss_osd_header_t;

struct rss_osd_shm {
    rss_osd_header_t *header;
    uint8_t *buf[2];
    size_t total_size;
    int shm_fd;
    int event_fd;
    bool is_producer;
    char name[64];
};

static size_t osd_total_size(uint32_t buf_size)
{
    return PAGE_SIZE + (size_t)buf_size * 2;
}

static void osd_set_pointers(rss_osd_shm_t *shm, void *base, uint32_t buf_size)
{
    shm->header = (rss_osd_header_t *)base;
    shm->buf[0] = (uint8_t *)base + PAGE_SIZE;
    shm->buf[1] = shm->buf[0] + buf_size;
}

static void make_shm_name(char *buf, size_t bufsz, const char *name)
{
    snprintf(buf, bufsz, "%s%s", RSS_OSD_SHM_PREFIX, name);
}

/* ------------------------------------------------------------------ */
/*  Producer API (ROD)                                                */
/* ------------------------------------------------------------------ */

rss_osd_shm_t *rss_osd_create(const char *name, uint32_t width, uint32_t height)
{
    if (!name || width == 0 || height == 0)
        return NULL;

    rss_osd_shm_t *shm = calloc(1, sizeof(*shm));
    if (!shm)
        return NULL;

    snprintf(shm->name, sizeof(shm->name), "%s", name);
    shm->is_producer = true;
    shm->shm_fd = -1;
    shm->event_fd = -1;

    uint32_t stride = width * 4; /* BGRA */
    uint32_t buf_size = stride * height;

    shm->total_size = osd_total_size(buf_size);

    char shm_name[128];
    make_shm_name(shm_name, sizeof(shm_name), name);

    shm->shm_fd = shm_open(shm_name, O_CREAT | O_RDWR | O_TRUNC, 0666);
    if (shm->shm_fd < 0)
        goto fail;

    if (ftruncate(shm->shm_fd, (off_t)shm->total_size) < 0)
        goto fail;

    void *base = mmap(NULL, shm->total_size, PROT_READ | PROT_WRITE, MAP_SHARED, shm->shm_fd, 0);
    if (base == MAP_FAILED)
        goto fail;

    osd_set_pointers(shm, base, buf_size);

    /* Initialise header. */
    memset(shm->header, 0, PAGE_SIZE);
    shm->header->width = width;
    shm->header->height = height;
    shm->header->stride = stride;
    shm->header->buf_size = buf_size;
    atomic_store_explicit(&shm->header->active_buf, 0, memory_order_relaxed);
    atomic_store_explicit(&shm->header->dirty, 0, memory_order_relaxed);
    shm->header->magic = RSS_OSD_MAGIC;
    shm->header->version = RSS_OSD_VERSION;
    shm->header->_reserved = 0;

    /* Clear both buffers. */
    memset(shm->buf[0], 0, buf_size);
    memset(shm->buf[1], 0, buf_size);

    /* Create eventfd for consumer notification. */
    shm->event_fd = eventfd(0, EFD_NONBLOCK);
    if (shm->event_fd < 0)
        goto fail;

    return shm;

fail:
    if (shm->shm_fd >= 0) {
        shm_unlink(shm_name);
        close(shm->shm_fd);
    }
    free(shm);
    return NULL;
}

void rss_osd_destroy(rss_osd_shm_t *shm)
{
    if (!shm)
        return;

    if (shm->header && shm->header != (void *)MAP_FAILED)
        munmap(shm->header, shm->total_size);

    if (shm->event_fd >= 0)
        close(shm->event_fd);

    if (shm->shm_fd >= 0) {
        char shm_name[128];
        make_shm_name(shm_name, sizeof(shm_name), shm->name);
        shm_unlink(shm_name);
        close(shm->shm_fd);
    }

    free(shm);
}

uint8_t *rss_osd_get_draw_buffer(rss_osd_shm_t *shm)
{
    if (!shm || !shm->is_producer)
        return NULL;

    /* Return the inactive buffer: the one the consumer is NOT reading. */
    int active = atomic_load_explicit(&shm->header->active_buf, memory_order_relaxed);
    return shm->buf[1 - active];
}

void rss_osd_publish(rss_osd_shm_t *shm)
{
    if (!shm || !shm->is_producer)
        return;

    /* Swap: the buffer we just drew into becomes the active one. */
    int active = atomic_load_explicit(&shm->header->active_buf, memory_order_relaxed);
    int new_active = 1 - active;

    /* Release ordering: all bitmap writes must be visible to the
     * consumer before it sees the new active_buf. */
    atomic_store_explicit(&shm->header->active_buf, new_active, memory_order_release);
    atomic_store_explicit(&shm->header->dirty, 1, memory_order_release);

    /* Signal the consumer via eventfd. */
    uint64_t val = 1;
    ssize_t r;
    do {
        r = write(shm->event_fd, &val, sizeof(val));
    } while (r < 0 && errno == EINTR);
}

/* ------------------------------------------------------------------ */
/*  Consumer API (RVD)                                                */
/* ------------------------------------------------------------------ */

rss_osd_shm_t *rss_osd_open(const char *name)
{
    if (!name)
        return NULL;

    rss_osd_shm_t *shm = calloc(1, sizeof(*shm));
    if (!shm)
        return NULL;

    snprintf(shm->name, sizeof(shm->name), "%s", name);
    shm->is_producer = false;
    shm->shm_fd = -1;
    shm->event_fd = -1;

    char shm_name[128];
    make_shm_name(shm_name, sizeof(shm_name), name);

    shm->shm_fd = shm_open(shm_name, O_RDWR, 0);
    if (shm->shm_fd < 0)
        goto fail;

    struct stat st;
    if (fstat(shm->shm_fd, &st) < 0)
        goto fail;

    shm->total_size = (size_t)st.st_size;

    /* Consumer needs PROT_WRITE for clear_dirty (atomic_store on dirty flag) */
    void *base = mmap(NULL, shm->total_size, PROT_READ | PROT_WRITE, MAP_SHARED, shm->shm_fd, 0);
    if (base == MAP_FAILED)
        goto fail;

    rss_osd_header_t *hdr = (rss_osd_header_t *)base;
    if (hdr->magic != RSS_OSD_MAGIC || hdr->version != RSS_OSD_VERSION) {
        munmap(base, shm->total_size);
        goto fail;
    }

    osd_set_pointers(shm, base, hdr->buf_size);

    /* Cross-process notification uses the atomic dirty flag in SHM,
     * NOT eventfd (eventfd is per-process, not shared). Consumer
     * polls rss_osd_check_dirty() which loads the atomic flag.
     * The eventfd here is unused but kept for potential future use. */
    shm->event_fd = eventfd(0, EFD_NONBLOCK);

    return shm;

fail:
    if (shm->shm_fd >= 0)
        close(shm->shm_fd);
    free(shm);
    return NULL;
}

void rss_osd_close(rss_osd_shm_t *shm)
{
    if (!shm)
        return;

    if (shm->header && shm->header != (void *)MAP_FAILED)
        munmap(shm->header, shm->total_size);

    if (shm->event_fd >= 0)
        close(shm->event_fd);
    if (shm->shm_fd >= 0)
        close(shm->shm_fd);

    free(shm);
}

const uint8_t *rss_osd_get_active_buffer(rss_osd_shm_t *shm, uint32_t *width, uint32_t *height)
{
    if (!shm)
        return NULL;

    /* Acquire ordering: see the bitmap data that the producer wrote
     * before setting active_buf. */
    int active = atomic_load_explicit(&shm->header->active_buf, memory_order_acquire);

    if (width)
        *width = shm->header->width;
    if (height)
        *height = shm->header->height;

    return shm->buf[active];
}

int rss_osd_check_dirty(rss_osd_shm_t *shm)
{
    if (!shm)
        return 0;

    return atomic_load_explicit(&shm->header->dirty, memory_order_acquire);
}

void rss_osd_clear_dirty(rss_osd_shm_t *shm)
{
    if (!shm)
        return;

    atomic_store_explicit(&shm->header->dirty, 0, memory_order_relaxed);
}

void rss_osd_heartbeat(rss_osd_shm_t *shm)
{
    if (!shm || !shm->is_producer)
        return;
    /* Set dirty without swapping buffers — tells consumer we're alive
     * without changing displayed content. */
    atomic_store_explicit(&shm->header->dirty, 1, memory_order_release);
}

int rss_osd_get_fd(rss_osd_shm_t *shm)
{
    return shm ? shm->shm_fd : -1;
}

int rss_osd_get_eventfd(rss_osd_shm_t *shm)
{
    return shm ? shm->event_fd : -1;
}
