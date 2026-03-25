# raptor-ipc

IPC library for the Raptor Streaming System (RSS). Provides lock-free shared memory frame transport, OSD bitmap double-buffering, and Unix domain socket control messaging between RSS daemons on Ingenic SoCs.

## Components

### SHM Ring Buffer (`rss_ring`)

Lock-free single-producer multi-consumer ring buffer over POSIX shared memory. Used by RVD (video daemon) to distribute encoded frames to RSD (RTSP), RAD (audio), and other consumers with zero-copy reads.

- Circular data region with contiguous frame writes (no split across wrap boundary)
- Slot metadata: sequence number, timestamp, NAL type, keyframe flag
- Futex-based consumer wakeup (cross-process, no eventfd)
- Sequence validation with post-copy re-check to detect producer overwrites
- Consumer-to-producer IDR request via atomic flag
- Stream info in header: codec, resolution, framerate, H.264 profile/level
- Incarnation counter for crash/restart detection

SHM names: `/rss_ring_<name>`

### OSD SHM Double-Buffer (`rss_osd_shm`)

Double-buffered BGRA bitmap transport for OSD overlays. ROD (OSD daemon) renders into the inactive buffer, atomically swaps, and signals RVD.

- Two BGRA buffers (stride = width * 4) behind a page-aligned header
- Atomic buffer swap with release/acquire ordering
- Dirty flag for polling, eventfd for notification
- Consumer reads active buffer while producer draws to inactive

SHM names: `/rss_osd_<name>`

### Control Socket (`rss_ctrl`)

Synchronous request/response over Unix domain sockets. Used by `raptorctl` to send commands to daemons.

- Wire protocol: 2-byte big-endian length prefix + JSON body (max 65535 bytes)
- One request, one response per connection
- Server: `listen` / `accept_and_handle` with callback
- Client: `send_command` with configurable timeout

## Build

```
make CC=mipsel-linux-gnu-gcc AR=mipsel-linux-gnu-ar
```

Produces `librss_ipc.a`. Link with `-lrt -lpthread`.

To clean: `make clean`

## API

### Ring Buffer

```c
/* Producer */
rss_ring_t *rss_ring_create(const char *name, uint32_t slot_count, uint32_t data_size);
void        rss_ring_destroy(rss_ring_t *ring);
int         rss_ring_publish(rss_ring_t *ring, const uint8_t *data, uint32_t length,
                             int64_t timestamp, uint16_t nal_type, uint8_t is_key);
int         rss_ring_publish_iov(rss_ring_t *ring, const rss_iov_t *iov, uint32_t iov_count,
                                 int64_t timestamp, uint16_t nal_type, uint8_t is_key);
void        rss_ring_set_stream_info(rss_ring_t *ring, uint32_t stream_id, uint32_t codec,
                                     uint32_t width, uint32_t height, uint32_t fps_num,
                                     uint32_t fps_den, uint8_t profile, uint8_t level);

/* Consumer */
rss_ring_t *rss_ring_open(const char *name);
void        rss_ring_close(rss_ring_t *ring);
int         rss_ring_read(rss_ring_t *ring, uint64_t *read_seq, uint8_t *dest,
                          uint32_t dest_size, uint32_t *length, rss_ring_slot_t *meta);
int         rss_ring_wait(rss_ring_t *ring, uint32_t timeout_ms);
const rss_ring_header_t *rss_ring_get_header(rss_ring_t *ring);

/* IDR Request (consumer sets, producer checks and clears) */
void rss_ring_request_idr(rss_ring_t *ring);
int  rss_ring_check_idr(rss_ring_t *ring);
```

`rss_ring_read` returns: `0` success, `-EAGAIN` no new data, `-ENOSPC` frame too large for buffer, `RSS_EOVERFLOW` (-75) consumer fell behind.

### OSD SHM

```c
/* Producer (ROD) */
rss_osd_shm_t *rss_osd_create(const char *name, uint32_t width, uint32_t height);
void            rss_osd_destroy(rss_osd_shm_t *shm);
uint8_t        *rss_osd_get_draw_buffer(rss_osd_shm_t *shm);
void            rss_osd_publish(rss_osd_shm_t *shm);

/* Consumer (RVD) */
rss_osd_shm_t *rss_osd_open(const char *name);
void            rss_osd_close(rss_osd_shm_t *shm);
const uint8_t  *rss_osd_get_active_buffer(rss_osd_shm_t *shm, uint32_t *width, uint32_t *height);
int             rss_osd_check_dirty(rss_osd_shm_t *shm);
void            rss_osd_clear_dirty(rss_osd_shm_t *shm);
int             rss_osd_get_eventfd(rss_osd_shm_t *shm);
```

### Control Socket

```c
/* Server (daemon) */
rss_ctrl_t *rss_ctrl_listen(const char *sock_path);
void        rss_ctrl_destroy(rss_ctrl_t *ctrl);
int         rss_ctrl_get_fd(rss_ctrl_t *ctrl);
int         rss_ctrl_accept_and_handle(rss_ctrl_t *ctrl,
                int (*handler)(const char *cmd_json, char *resp_buf,
                               int resp_buf_size, void *userdata),
                void *userdata);

/* Client (raptorctl) */
int rss_ctrl_send_command(const char *sock_path, const char *cmd_json,
                          char *resp_buf, int resp_buf_size, uint32_t timeout_ms);
```

## Requirements

- C11 compiler with `_Atomic` support
- Linux (futex, eventfd, POSIX SHM)
- No vendor SDK dependencies

## License

Licensed under the GNU General Public License v3.0.
