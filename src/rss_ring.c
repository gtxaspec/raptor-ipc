/*
 * rss_ring.c -- SHM ring buffer implementation.
 *
 * Lock-free, single-producer multi-consumer ring buffer over POSIX SHM.
 *
 * Memory layout (single contiguous mmap):
 *
 *   +-------------------+  offset 0
 *   | rss_ring_header_t |  control block (cache-line aligned)
 *   +-------------------+  offset PAGE_SIZE (page-aligned)
 *   | slot[0]           |  rss_ring_slot_t
 *   | slot[1]           |
 *   | ...               |
 *   | slot[N-1]         |
 *   +-------------------+  offset PAGE_SIZE + N * sizeof(slot)
 *   | data region       |  raw frame payload storage (circular)
 *   +-------------------+
 *
 * The data region is circular. Frames are written contiguously; if the
 * remaining space before the end of the region is less than the frame
 * size, the tail is skipped and the frame is written at offset 0.
 * This guarantees contiguous reads.
 */

#include "rss_ipc.h"

#include <errno.h>
#include <fcntl.h>
#include <poll.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/eventfd.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <time.h>
#include <unistd.h>

/* Page size for alignment. */
#ifndef PAGE_SIZE
#define PAGE_SIZE 4096
#endif

#define ALIGN_UP(x, a) (((x) + (a) - 1) & ~((a) - 1))

struct rss_ring {
    rss_ring_header_t *header;
    rss_ring_slot_t   *slots;
    uint8_t           *data;
    size_t             total_size;
    int                shm_fd;
    int                event_fd;
    bool               is_producer;
    char               name[64];
};

/*
 * Compute mmap layout offsets:
 *   header:  0 .. PAGE_SIZE-1
 *   slots:   PAGE_SIZE .. PAGE_SIZE + slot_count*sizeof(slot) - 1
 *   data:    PAGE_SIZE + slot_count*sizeof(slot) .. end
 */
static size_t ring_total_size(uint32_t slot_count, uint32_t data_size)
{
    size_t slots_bytes = (size_t)slot_count * sizeof(rss_ring_slot_t);
    return PAGE_SIZE + slots_bytes + data_size;
}

static void ring_set_pointers(rss_ring_t *ring, void *base,
                               uint32_t slot_count)
{
    ring->header = (rss_ring_header_t *)base;
    ring->slots  = (rss_ring_slot_t *)((uint8_t *)base + PAGE_SIZE);
    ring->data   = (uint8_t *)ring->slots +
                   (size_t)slot_count * sizeof(rss_ring_slot_t);
}

static void make_shm_name(char *buf, size_t bufsz, const char *name)
{
    snprintf(buf, bufsz, "%s%s", RSS_RING_SHM_PREFIX, name);
}

/* ------------------------------------------------------------------ */
/*  Producer API                                                      */
/* ------------------------------------------------------------------ */

rss_ring_t *rss_ring_create(const char *name, uint32_t slot_count,
                             uint32_t data_size)
{
    if (!name || slot_count == 0 || slot_count > RSS_RING_MAX_SLOTS ||
        data_size == 0)
        return NULL;

    /* slot_count must be a power of 2 */
    if ((slot_count & (slot_count - 1)) != 0)
        return NULL;

    rss_ring_t *ring = calloc(1, sizeof(*ring));
    if (!ring)
        return NULL;

    snprintf(ring->name, sizeof(ring->name), "%s", name);
    ring->is_producer = true;
    ring->shm_fd      = -1;
    ring->event_fd    = -1;

    char shm_name[128];
    make_shm_name(shm_name, sizeof(shm_name), name);

    ring->total_size = ring_total_size(slot_count, data_size);

    /* Create SHM segment. */
    ring->shm_fd = shm_open(shm_name, O_CREAT | O_RDWR | O_TRUNC, 0666);
    if (ring->shm_fd < 0)
        goto fail;

    if (ftruncate(ring->shm_fd, (off_t)ring->total_size) < 0)
        goto fail;

    void *base = mmap(NULL, ring->total_size,
                       PROT_READ | PROT_WRITE,
                       MAP_SHARED, ring->shm_fd, 0);
    if (base == MAP_FAILED)
        goto fail;

    ring_set_pointers(ring, base, slot_count);

    /* Initialise header. */
    memset(ring->header, 0, PAGE_SIZE);
    atomic_store_explicit(&ring->header->write_seq, 0, memory_order_relaxed);
    ring->header->slot_count = slot_count;
    ring->header->data_size  = data_size;
    atomic_store_explicit(&ring->header->data_head, 0, memory_order_relaxed);
    ring->header->magic   = RSS_RING_MAGIC;
    ring->header->version = RSS_RING_VERSION;

    /* Initialise all slot sequences to 0 (no valid data). */
    for (uint32_t i = 0; i < slot_count; i++)
        atomic_store_explicit(&ring->slots[i].seq, 0, memory_order_relaxed);

    /* Create eventfd for consumer notification.
     * EFD_NONBLOCK: producers never block on write.
     * EFD_SEMAPHORE: each consumer read decrements by 1, so multiple
     *                consumers can poll independently. */
    ring->event_fd = eventfd(0, EFD_NONBLOCK | EFD_SEMAPHORE);
    if (ring->event_fd < 0)
        goto fail;

    return ring;

fail:
    if (ring->shm_fd >= 0) {
        shm_unlink(shm_name);
        close(ring->shm_fd);
    }
    free(ring);
    return NULL;
}

void rss_ring_destroy(rss_ring_t *ring)
{
    if (!ring)
        return;

    if (ring->header && ring->header != MAP_FAILED)
        munmap(ring->header, ring->total_size);

    if (ring->event_fd >= 0)
        close(ring->event_fd);

    if (ring->shm_fd >= 0) {
        char shm_name[128];
        make_shm_name(shm_name, sizeof(shm_name), ring->name);
        shm_unlink(shm_name);
        close(ring->shm_fd);
    }

    free(ring);
}

int rss_ring_publish(rss_ring_t *ring,
                      const uint8_t *data, uint32_t length,
                      int64_t timestamp, uint16_t nal_type,
                      uint8_t is_key)
{
    if (!ring || !ring->is_producer || !data || length == 0)
        return -EINVAL;

    rss_ring_header_t *hdr = ring->header;
    uint32_t data_size  = hdr->data_size;
    uint32_t slot_count = hdr->slot_count;

    if (length > data_size)
        return -ENOSPC;

    /* Allocate space in the circular data region.
     * If the frame doesn't fit before the end, skip to offset 0. */
    uint32_t head = atomic_load_explicit(&hdr->data_head,
                                          memory_order_relaxed);
    uint32_t remaining = data_size - head;
    uint32_t offset;

    if (length <= remaining) {
        offset = head;
        atomic_store_explicit(&hdr->data_head, head + length,
                               memory_order_relaxed);
    } else {
        /* Not enough room at tail -- wrap to beginning. */
        offset = 0;
        atomic_store_explicit(&hdr->data_head, length,
                               memory_order_relaxed);
    }

    /* Copy frame payload into the data region. */
    memcpy(ring->data + offset, data, length);

    /* Determine the slot and sequence number. */
    uint64_t seq = atomic_load_explicit(&hdr->write_seq,
                                         memory_order_relaxed) + 1;
    uint32_t slot_idx = (uint32_t)(seq % slot_count);

    rss_ring_slot_t *slot = &ring->slots[slot_idx];
    slot->data_offset = offset;
    slot->data_length = length;
    slot->timestamp   = timestamp;
    slot->nal_type    = nal_type;
    slot->is_key      = is_key;
    slot->_pad        = 0;

    /* Write the slot sequence (used by consumers to validate reads). */
    atomic_store_explicit(&slot->seq, seq, memory_order_relaxed);

    /* Publish: release fence then store write_seq.
     * All preceding writes (data copy, slot fill) are visible to
     * consumers that load write_seq with acquire. */
    atomic_store_explicit(&hdr->write_seq, seq, memory_order_release);

    /* Notify consumers via eventfd. */
    uint64_t val = 1;
    ssize_t r;
    do {
        r = write(ring->event_fd, &val, sizeof(val));
    } while (r < 0 && errno == EINTR);
    /* EAGAIN is fine -- counter already non-zero, consumers will wake. */

    return 0;
}

void rss_ring_set_stream_info(rss_ring_t *ring, uint32_t stream_id,
                               uint32_t codec, uint32_t width,
                               uint32_t height, uint32_t fps_num,
                               uint32_t fps_den)
{
    if (!ring || !ring->is_producer)
        return;

    ring->header->stream_id = stream_id;
    ring->header->codec     = codec;
    ring->header->width     = width;
    ring->header->height    = height;
    ring->header->fps_num   = fps_num;
    ring->header->fps_den   = fps_den;
}

int rss_ring_get_eventfd(rss_ring_t *ring)
{
    return ring ? ring->event_fd : -1;
}

/* ------------------------------------------------------------------ */
/*  Consumer API                                                      */
/* ------------------------------------------------------------------ */

rss_ring_t *rss_ring_open(const char *name)
{
    if (!name)
        return NULL;

    rss_ring_t *ring = calloc(1, sizeof(*ring));
    if (!ring)
        return NULL;

    snprintf(ring->name, sizeof(ring->name), "%s", name);
    ring->is_producer = false;
    ring->shm_fd      = -1;
    ring->event_fd    = -1;

    char shm_name[128];
    make_shm_name(shm_name, sizeof(shm_name), name);

    ring->shm_fd = shm_open(shm_name, O_RDONLY, 0);
    if (ring->shm_fd < 0)
        goto fail;

    /* Stat to get the total size. */
    struct stat st;
    if (fstat(ring->shm_fd, &st) < 0)
        goto fail;

    ring->total_size = (size_t)st.st_size;

    /* Map the entire segment read-only. Consumers only read. */
    void *base = mmap(NULL, ring->total_size,
                       PROT_READ, MAP_SHARED, ring->shm_fd, 0);
    if (base == MAP_FAILED)
        goto fail;

    /* Peek at the header to read slot_count before setting pointers. */
    rss_ring_header_t *hdr = (rss_ring_header_t *)base;
    if (hdr->magic != RSS_RING_MAGIC || hdr->version != RSS_RING_VERSION) {
        munmap(base, ring->total_size);
        goto fail;
    }

    ring_set_pointers(ring, base, hdr->slot_count);

    /* Consumer creates its own eventfd -- the producer eventfd is not
     * shared across the SHM boundary. For notification, consumers should
     * either:
     *   1. Receive the eventfd via SCM_RIGHTS (daemon startup), or
     *   2. Poll the write_seq field directly.
     *
     * We create a local eventfd for rss_ring_wait() compatibility.
     * In practice, the daemon framework will replace this via
     * rss_ring_get_eventfd() after receiving the fd from the producer. */
    ring->event_fd = eventfd(0, EFD_NONBLOCK | EFD_SEMAPHORE);
    if (ring->event_fd < 0) {
        munmap(base, ring->total_size);
        goto fail;
    }

    return ring;

fail:
    if (ring->shm_fd >= 0)
        close(ring->shm_fd);
    free(ring);
    return NULL;
}

void rss_ring_close(rss_ring_t *ring)
{
    if (!ring)
        return;

    if (ring->header && ring->header != MAP_FAILED)
        munmap(ring->header, ring->total_size);

    if (ring->event_fd >= 0)
        close(ring->event_fd);
    if (ring->shm_fd >= 0)
        close(ring->shm_fd);

    free(ring);
}

int rss_ring_read(rss_ring_t *ring, uint64_t *read_seq,
                   const uint8_t **data, uint32_t *length,
                   rss_ring_slot_t *meta)
{
    if (!ring || !read_seq || !data || !length)
        return -EINVAL;

    rss_ring_header_t *hdr = ring->header;
    uint32_t slot_count = hdr->slot_count;

    /* Load write_seq with acquire -- pairs with the producer's release
     * store, ensuring all slot and data writes are visible. */
    uint64_t wseq = atomic_load_explicit(&hdr->write_seq,
                                          memory_order_acquire);

    if (*read_seq >= wseq) {
        /* No new frames available. */
        return -EAGAIN;
    }

    /* Check for overflow: consumer fell behind by >= slot_count frames.
     * The oldest valid sequence is (wseq - slot_count + 1). */
    if (wseq - *read_seq >= slot_count) {
        *read_seq = wseq - slot_count + 1;
        /* Return overflow but still provide the oldest valid frame. */
        uint32_t idx = (uint32_t)(*read_seq % slot_count);
        const rss_ring_slot_t *slot = &ring->slots[idx];

        *data   = ring->data + slot->data_offset;
        *length = slot->data_length;
        if (meta)
            *meta = *slot;

        (*read_seq)++;
        return RSS_EOVERFLOW;
    }

    /* Normal read: consume the next frame at read_seq. */
    uint32_t idx = (uint32_t)(*read_seq % slot_count);
    const rss_ring_slot_t *slot = &ring->slots[idx];

    /* Validate that the slot's sequence matches what we expect.
     * A mismatch means the producer has wrapped past us in the slot
     * array -- treat as overflow. */
    uint64_t slot_seq = atomic_load_explicit(
        (_Atomic uint64_t *)&slot->seq, memory_order_acquire);
    if (slot_seq != *read_seq) {
        /* Slot was reused. Advance to oldest valid. */
        *read_seq = wseq - slot_count + 1;
        idx = (uint32_t)(*read_seq % slot_count);
        slot = &ring->slots[idx];

        *data   = ring->data + slot->data_offset;
        *length = slot->data_length;
        if (meta)
            *meta = *slot;

        (*read_seq)++;
        return RSS_EOVERFLOW;
    }

    *data   = ring->data + slot->data_offset;
    *length = slot->data_length;
    if (meta)
        *meta = *slot;

    /* Re-validate: check that the producer hasn't overwritten this slot
     * while we were reading its metadata. If the seq changed, the data
     * region is potentially corrupt. */
    uint64_t recheck = atomic_load_explicit(
        (_Atomic uint64_t *)&slot->seq, memory_order_acquire);
    if (recheck != slot_seq) {
        *read_seq = wseq;
        return RSS_EOVERFLOW;
    }

    (*read_seq)++;
    return 0;
}

int rss_ring_wait(rss_ring_t *ring, uint32_t timeout_ms)
{
    if (!ring)
        return -EINVAL;

    rss_ring_header_t *hdr = ring->header;

    /* For producer: poll its own eventfd (which it writes to). */
    if (ring->is_producer) {
        struct pollfd pfd = {
            .fd     = ring->event_fd,
            .events = POLLIN,
        };
        int ret;
        do {
            ret = poll(&pfd, 1, (int)timeout_ms);
        } while (ret < 0 && errno == EINTR);
        if (ret < 0) return -errno;
        if (ret == 0) return -ETIMEDOUT;
        uint64_t val;
        ssize_t r;
        do { r = read(ring->event_fd, &val, sizeof(val)); }
        while (r < 0 && errno == EINTR);
        return 0;
    }

    /* For consumer: poll write_seq directly.
     * The consumer's eventfd is not connected to the producer's eventfd
     * (they are in separate processes). Instead, we poll the shared
     * write_seq field with short sleeps. */
    uint64_t last_seq = atomic_load_explicit(&hdr->write_seq,
                                              memory_order_acquire);
    uint32_t elapsed = 0;
    uint32_t interval = 1;  /* start at 1ms, ramp up */

    while (elapsed < timeout_ms) {
        struct timespec ts = {
            .tv_sec  = 0,
            .tv_nsec = interval * 1000000L
        };
        nanosleep(&ts, NULL);
        elapsed += interval;
        if (interval < 10)
            interval++;

        uint64_t seq = atomic_load_explicit(&hdr->write_seq,
                                             memory_order_acquire);
        if (seq != last_seq)
            return 0;
    }

    return -ETIMEDOUT;
}

const rss_ring_header_t *rss_ring_get_header(rss_ring_t *ring)
{
    return ring ? ring->header : NULL;
}
