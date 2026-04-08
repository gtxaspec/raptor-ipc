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
#include <signal.h>
#include <limits.h>
#include <linux/futex.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/syscall.h>
#include <time.h>
#include <unistd.h>

/* Cross-process futex on SHM: FUTEX_WAIT/FUTEX_WAKE without FUTEX_PRIVATE_FLAG.
 * Private flag would use process-local hash — must NOT be set for shared memory. */
static int futex_wait(uint32_t *addr, uint32_t expected, const struct timespec *timeout)
{
    return (int)syscall(SYS_futex, addr, FUTEX_WAIT, expected, timeout, NULL, 0);
}

static int futex_wake(uint32_t *addr, int count)
{
    return (int)syscall(SYS_futex, addr, FUTEX_WAKE, count, NULL, NULL, 0);
}

/* Page size for alignment. */
#ifndef PAGE_SIZE
#define PAGE_SIZE 4096
#endif

#define ALIGN_UP(x, a) (((x) + (a) - 1) & ~((a) - 1))

struct rss_ring {
    rss_ring_header_t *header;
    rss_ring_slot_t *slots;
    uint32_t open_incarnation; /* incarnation at time of open */
    uint8_t *data;
    size_t total_size;
    int shm_fd;
    bool is_producer;
    char name[64];
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

static void ring_set_pointers(rss_ring_t *ring, void *base, uint32_t slot_count)
{
    ring->header = (rss_ring_header_t *)base;
    ring->slots = (rss_ring_slot_t *)((uint8_t *)base + PAGE_SIZE);
    ring->data = (uint8_t *)ring->slots + (size_t)slot_count * sizeof(rss_ring_slot_t);
}

static void make_shm_name(char *buf, size_t bufsz, const char *name)
{
    snprintf(buf, bufsz, "%s%s", RSS_RING_SHM_PREFIX, name);
}

/* ------------------------------------------------------------------ */
/*  Producer API                                                      */
/* ------------------------------------------------------------------ */

rss_ring_t *rss_ring_create(const char *name, uint32_t slot_count, uint32_t data_size)
{
    if (!name || slot_count == 0 || slot_count > RSS_RING_MAX_SLOTS || data_size == 0)
        return NULL;

    /* slot_count must be a power of 2 */
    if ((slot_count & (slot_count - 1)) != 0)
        return NULL;

    rss_ring_t *ring = calloc(1, sizeof(*ring));
    if (!ring)
        return NULL;

    snprintf(ring->name, sizeof(ring->name), "%s", name);
    ring->is_producer = true;
    ring->shm_fd = -1;

    char shm_name[128];
    make_shm_name(shm_name, sizeof(shm_name), name);

    ring->total_size = ring_total_size(slot_count, data_size);

    /* Create SHM segment. */
    ring->shm_fd = shm_open(shm_name, O_CREAT | O_RDWR | O_TRUNC, 0666);
    if (ring->shm_fd < 0)
        goto fail;

    if (ftruncate(ring->shm_fd, (off_t)ring->total_size) < 0)
        goto fail;

    void *base = mmap(NULL, ring->total_size, PROT_READ | PROT_WRITE, MAP_SHARED, ring->shm_fd, 0);
    if (base == MAP_FAILED)
        goto fail;

    ring_set_pointers(ring, base, slot_count);

    /* Initialise header. Magic written LAST with release ordering
     * so consumers see all fields initialized before magic becomes valid. */
    memset(ring->header, 0, PAGE_SIZE);
    atomic_store_explicit(&ring->header->write_seq, 0, memory_order_relaxed);
    ring->header->slot_count = slot_count;
    ring->header->data_size = data_size;
    atomic_store_explicit(&ring->header->data_head, 0, memory_order_relaxed);
    ring->header->version = RSS_RING_VERSION;
    atomic_store_explicit(&ring->header->incarnation,
                          atomic_load_explicit(&ring->header->incarnation, memory_order_relaxed) +
                              1,
                          memory_order_relaxed);

    /* Initialise all slot sequences to 0 (no valid data). */
    for (uint32_t i = 0; i < slot_count; i++)
        atomic_store_explicit(&ring->slots[i].seq, 0, memory_order_relaxed);

    /* Write magic last — release fence ensures all above writes are
     * visible to consumers before they see valid magic. */
    atomic_store_explicit(&ring->header->magic, RSS_RING_MAGIC, memory_order_release);

    /* Producer uses futex on write_seq for consumer notification. */

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

    if (ring->shm_fd >= 0) {
        char shm_name[128];
        make_shm_name(shm_name, sizeof(shm_name), ring->name);
        shm_unlink(shm_name);
        close(ring->shm_fd);
    }

    free(ring);
}

int rss_ring_publish_iov(rss_ring_t *ring, const rss_iov_t *iov, uint32_t iov_count,
                         int64_t timestamp, uint16_t nal_type, uint8_t is_key)
{
    if (!ring || !ring->is_producer || !iov || iov_count == 0)
        return -EINVAL;

    /* Compute total length with overflow check */
    uint32_t length = 0;
    for (uint32_t i = 0; i < iov_count; i++) {
        if (__builtin_add_overflow(length, iov[i].length, &length))
            return -EOVERFLOW;
    }

    if (length == 0)
        return -EINVAL;

    rss_ring_header_t *hdr = ring->header;
    uint32_t data_size = hdr->data_size;
    uint32_t slot_count = hdr->slot_count;

    /* Frame larger than entire data region — reject. */
    if (length > data_size)
        return -ENOSPC;

    /* Allocate space in the circular data region.
     * If the frame doesn't fit in the remaining tail, skip to offset 0
     * (waste the tail, bounded at one frame per wrap). Frames are NOT
     * split across the wrap boundary — each frame is contiguous.
     * Consumers MUST follow slots in sequence order; data_offset is
     * not monotonically increasing due to wrap-back. */
    uint32_t head = atomic_load_explicit(&hdr->data_head, memory_order_relaxed);
    uint32_t remaining = data_size - head;
    uint32_t offset;

    if (length <= remaining) {
        offset = head;
        atomic_store_explicit(&hdr->data_head, head + length, memory_order_relaxed);
    } else {
        offset = 0;
        atomic_store_explicit(&hdr->data_head, length, memory_order_relaxed);
    }

    /* Copy iov segments contiguously into the data region. */
    uint32_t off = offset;
    for (uint32_t i = 0; i < iov_count; i++) {
        memcpy(ring->data + off, iov[i].data, iov[i].length);
        off += iov[i].length;
    }

    /* Determine the slot and sequence number. */
    uint64_t seq = atomic_load_explicit(&hdr->write_seq, memory_order_relaxed) + 1;
    uint32_t slot_idx = (uint32_t)(seq % slot_count);

    rss_ring_slot_t *slot = &ring->slots[slot_idx];
    slot->data_offset = offset;
    slot->data_length = length;
    slot->timestamp = timestamp;
    slot->nal_type = nal_type;
    slot->is_key = is_key;
    slot->_pad = 0;

    /* Write the slot sequence (used by consumers to validate reads). */
    atomic_store_explicit(&slot->seq, seq, memory_order_relaxed);

    /* Publish: release fence then store write_seq. */
    atomic_store_explicit(&hdr->write_seq, seq, memory_order_release);

    /* Wake all consumers waiting on write_seq via futex. */
    futex_wake((uint32_t *)&hdr->write_seq, INT_MAX);

    return 0;
}

int rss_ring_publish(rss_ring_t *ring, const uint8_t *data, uint32_t length, int64_t timestamp,
                     uint16_t nal_type, uint8_t is_key)
{
    rss_iov_t iov = {.data = data, .length = length};
    return rss_ring_publish_iov(ring, &iov, 1, timestamp, nal_type, is_key);
}

void rss_ring_set_stream_info(rss_ring_t *ring, uint32_t stream_id, uint32_t codec, uint32_t width,
                              uint32_t height, uint32_t fps_num, uint32_t fps_den, uint8_t profile,
                              uint8_t level)
{
    if (!ring || !ring->is_producer)
        return;

    ring->header->stream_id = stream_id;
    ring->header->codec = codec;
    ring->header->width = width;
    ring->header->height = height;
    ring->header->fps_num = fps_num;
    ring->header->fps_den = fps_den;
    ring->header->profile = profile;
    ring->header->level = level;
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
    ring->shm_fd = -1;

    char shm_name[128];
    make_shm_name(shm_name, sizeof(shm_name), name);

    /* O_RDWR needed for consumer IDR request (atomic write to header) */
    ring->shm_fd = shm_open(shm_name, O_RDWR, 0);
    if (ring->shm_fd < 0)
        goto fail;

    /* Stat to get the total size. */
    struct stat st;
    if (fstat(ring->shm_fd, &st) < 0)
        goto fail;

    ring->total_size = (size_t)st.st_size;

    /* PROT_WRITE needed for IDR request flag (atomic store to header) */
    void *base = mmap(NULL, ring->total_size, PROT_READ | PROT_WRITE, MAP_SHARED, ring->shm_fd, 0);
    if (base == MAP_FAILED)
        goto fail;

    /* Peek at the header — acquire-load magic to ensure all header
     * fields are visible (pairs with producer's release store). */
    rss_ring_header_t *hdr = (rss_ring_header_t *)base;
    uint32_t m = atomic_load_explicit(&hdr->magic, memory_order_acquire);
    if (m != RSS_RING_MAGIC || hdr->version != RSS_RING_VERSION) {
        munmap(base, ring->total_size);
        goto fail;
    }

    ring_set_pointers(ring, base, hdr->slot_count);
    ring->open_incarnation = atomic_load_explicit(&hdr->incarnation, memory_order_relaxed);

    /* Consumer uses futex on write_seq for notification — no eventfd needed. */

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

    if (ring->shm_fd >= 0)
        close(ring->shm_fd);

    free(ring);
}

int rss_ring_read(rss_ring_t *ring, uint64_t *read_seq, uint8_t *dest, uint32_t dest_size,
                  uint32_t *length, rss_ring_slot_t *meta)
{
    if (!ring || !read_seq || !dest || !length)
        return -EINVAL;

    rss_ring_header_t *hdr = ring->header;

    /* Check incarnation — if the producer recreated the ring, our
     * state (read_seq, mmap) is stale. Consumer must re-open. */
    uint32_t inc = atomic_load_explicit(&hdr->incarnation, memory_order_acquire);
    if (inc != ring->open_incarnation)
        return RSS_EOVERFLOW;

    uint32_t slot_count = hdr->slot_count;

    /* Load write_seq with acquire -- pairs with the producer's release
     * store, ensuring all slot and data writes are visible. */
    uint64_t wseq = atomic_load_explicit(&hdr->write_seq, memory_order_acquire);

    if (*read_seq >= wseq)
        return -EAGAIN;

    /* Check for overflow: consumer fell behind by >= slot_count frames. */
    if (wseq - *read_seq >= slot_count) {
        *read_seq = wseq;
        return RSS_EOVERFLOW;
    }

    /* Normal read: consume the next frame at read_seq. */
    uint32_t idx = (uint32_t)(*read_seq % slot_count);
    const rss_ring_slot_t *slot = &ring->slots[idx];

    /* Validate that the slot's sequence matches what we expect. */
    uint64_t slot_seq = atomic_load_explicit((_Atomic uint64_t *)&slot->seq, memory_order_acquire);
    if (slot_seq != *read_seq) {
        *read_seq = wseq;
        return RSS_EOVERFLOW;
    }

    /* Read slot metadata before copy (producer could recycle after). */
    uint32_t data_offset = slot->data_offset;
    uint32_t data_length = slot->data_length;

    if (data_length > dest_size) {
        /* Frame too large for caller's buffer — skip it. */
        (*read_seq)++;
        *length = data_length;
        return -ENOSPC;
    }

    /* Copy frame data into caller's buffer. */
    memcpy(dest, ring->data + data_offset, data_length);

    if (meta)
        *meta = *slot;

    *length = data_length;

    /* Re-validate AFTER copy: if the producer recycled this slot during
     * our memcpy, the data we copied may be corrupt. Discard it.
     *
     * ABA hazard note: if the producer wraps by exactly N * slot_count
     * during the memcpy, recheck == slot_seq + N*slot_count != slot_seq
     * (different value), so it IS detected. The only undetectable case
     * is wrap by exactly 2^64 — requires producing 2^64 frames during
     * a ~0.1ms memcpy, physically impossible at any real frame rate. */
    uint64_t recheck = atomic_load_explicit((_Atomic uint64_t *)&slot->seq, memory_order_acquire);
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

    /* Futex-wait on write_seq in shared memory.
     * Futex operates on the low 32 bits of write_seq (little-endian).
     * The producer calls futex_wake after each publish. */
    uint32_t *futex_addr = (uint32_t *)&hdr->write_seq;
    uint32_t expected = *futex_addr;

    struct timespec ts = {.tv_sec = timeout_ms / 1000, .tv_nsec = (timeout_ms % 1000) * 1000000L};

    int ret = futex_wait(futex_addr, expected, &ts);
    if (ret < 0 && errno == ETIMEDOUT)
        return -ETIMEDOUT;
    /* EAGAIN means value already changed — new data available. */
    return 0;
}

const rss_ring_header_t *rss_ring_get_header(rss_ring_t *ring)
{
    return ring ? ring->header : NULL;
}

void rss_ring_request_idr(rss_ring_t *ring)
{
    if (ring && ring->header)
        atomic_store_explicit(&ring->header->idr_request, 1, memory_order_relaxed);
}

int rss_ring_check_idr(rss_ring_t *ring)
{
    if (!ring || !ring->header)
        return 0;
    /* Atomic exchange: read and clear in one operation */
    return atomic_exchange_explicit(&ring->header->idr_request, 0, memory_order_relaxed);
}

void rss_ring_acquire(rss_ring_t *ring)
{
    if (!ring || !ring->header)
        return;
    atomic_fetch_add_explicit(&ring->header->reader_count, 1, memory_order_relaxed);

    /* Register PID for crash detection */
    uint32_t pid = (uint32_t)getpid();
    for (int i = 0; i < RSS_RING_MAX_READERS; i++) {
        uint32_t expected = 0;
        if (atomic_compare_exchange_strong_explicit(&ring->header->reader_pids[i],
                                                    &expected, pid,
                                                    memory_order_relaxed,
                                                    memory_order_relaxed))
            break;
    }
}

void rss_ring_release(rss_ring_t *ring)
{
    if (!ring || !ring->header)
        return;
    atomic_fetch_sub_explicit(&ring->header->reader_count, 1, memory_order_release);

    /* Unregister PID */
    uint32_t pid = (uint32_t)getpid();
    for (int i = 0; i < RSS_RING_MAX_READERS; i++) {
        uint32_t expected = pid;
        if (atomic_compare_exchange_strong_explicit(&ring->header->reader_pids[i],
                                                    &expected, 0,
                                                    memory_order_relaxed,
                                                    memory_order_relaxed))
            break;
    }
}

uint32_t rss_ring_reader_count(rss_ring_t *ring)
{
    if (!ring || !ring->header)
        return 0;
    return atomic_load_explicit(&ring->header->reader_count, memory_order_relaxed);
}

uint32_t rss_ring_reap_dead_readers(rss_ring_t *ring)
{
    if (!ring || !ring->header)
        return 0;

    uint32_t reaped = 0;
    uint32_t live = 0;
    for (int i = 0; i < RSS_RING_MAX_READERS; i++) {
        uint32_t pid = atomic_load_explicit(&ring->header->reader_pids[i],
                                            memory_order_relaxed);
        if (pid == 0)
            continue;
        if (kill((pid_t)pid, 0) == -1 && errno == ESRCH) {
            /* Process is dead — clear slot */
            atomic_store_explicit(&ring->header->reader_pids[i], 0,
                                  memory_order_relaxed);
            reaped++;
        } else {
            live++;
        }
    }

    /* Reconcile: reset reader_count to match live PIDs.
     * Handles orphaned counts from unclean shutdowns. */
    uint32_t count = atomic_load_explicit(&ring->header->reader_count,
                                          memory_order_relaxed);
    if (count != live)
        atomic_store_explicit(&ring->header->reader_count, live,
                              memory_order_relaxed);

    return reaped + (count > live ? count - live : 0);
}
