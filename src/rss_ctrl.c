/*
 * rss_ctrl.c -- Control socket implementation.
 *
 * Wire protocol: 2-byte big-endian length prefix + JSON body.
 *
 *   +--------+--------+-----...-----+
 *   | len_hi | len_lo |  JSON body  |
 *   +--------+--------+-----...-----+
 *      2 bytes           len bytes
 *
 * The protocol is synchronous: one request, one response per connection.
 * raptorctl connects, sends a request, reads the response, and closes.
 */

#include "rss_ipc.h"

#include <errno.h>
#include <fcntl.h>
#include <poll.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <sys/un.h>
#include <unistd.h>

#define RSS_CTRL_MAX_MSG 65535 /* maximum JSON body size */
#define RSS_CTRL_BACKLOG 5

struct rss_ctrl {
    int listen_fd;
    char sock_path[108]; /* sun_path limit */
};

/* ------------------------------------------------------------------ */
/*  Helpers                                                           */
/* ------------------------------------------------------------------ */

/*
 * Read exactly `count` bytes from fd into buf.
 * Returns 0 on success, -errno on error, -ECONNRESET on EOF.
 */
static int read_exact(int fd, void *buf, size_t count, uint32_t timeout_ms)
{
    uint8_t *p = buf;
    size_t remaining = count;

    while (remaining > 0) {
        if (timeout_ms > 0) {
            struct pollfd pfd = {.fd = fd, .events = POLLIN};
            int pr;
            do {
                pr = poll(&pfd, 1, (int)timeout_ms);
            } while (pr < 0 && errno == EINTR);

            if (pr < 0)
                return -errno;
            if (pr == 0)
                return -ETIMEDOUT;
        }

        ssize_t n;
        do {
            n = read(fd, p, remaining);
        } while (n < 0 && errno == EINTR);

        if (n < 0)
            return -errno;
        if (n == 0)
            return -ECONNRESET;

        p += n;
        remaining -= (size_t)n;
    }

    return 0;
}

/*
 * Write exactly `count` bytes from buf to fd.
 * Returns 0 on success, -errno on error.
 */
static int write_exact(int fd, const void *buf, size_t count)
{
    const uint8_t *p = buf;
    size_t remaining = count;

    while (remaining > 0) {
        ssize_t n;
        do {
            n = write(fd, p, remaining);
        } while (n < 0 && errno == EINTR);

        if (n < 0)
            return -errno;

        p += n;
        remaining -= (size_t)n;
    }

    return 0;
}

/*
 * Read a length-prefixed message from fd.
 * Allocates the message buffer; caller must free().
 * Returns message length on success, -errno on error.
 */
static int read_message(int fd, char **out_buf, uint32_t timeout_ms)
{
    uint8_t len_buf[2];
    int ret = read_exact(fd, len_buf, 2, timeout_ms);
    if (ret < 0)
        return ret;

    uint16_t msg_len = ((uint16_t)len_buf[0] << 8) | len_buf[1];
    if (msg_len == 0) {
        *out_buf = NULL;
        return 0;
    }

    char *buf = malloc((size_t)msg_len + 1);
    if (!buf)
        return -ENOMEM;

    ret = read_exact(fd, buf, msg_len, timeout_ms);
    if (ret < 0) {
        free(buf);
        return ret;
    }

    buf[msg_len] = '\0';
    *out_buf = buf;
    return msg_len;
}

/*
 * Write a length-prefixed message to fd.
 * Returns 0 on success, -errno on error.
 */
static int write_message(int fd, const char *buf, size_t len)
{
    if (len > RSS_CTRL_MAX_MSG)
        return -EMSGSIZE;

    uint8_t len_buf[2];
    len_buf[0] = (uint8_t)(len >> 8);
    len_buf[1] = (uint8_t)(len & 0xFF);

    int ret = write_exact(fd, len_buf, 2);
    if (ret < 0)
        return ret;

    return write_exact(fd, buf, len);
}

/* ------------------------------------------------------------------ */
/*  Server API (daemon side)                                          */
/* ------------------------------------------------------------------ */

rss_ctrl_t *rss_ctrl_listen(const char *sock_path)
{
    if (!sock_path)
        return NULL;

    rss_ctrl_t *ctrl = calloc(1, sizeof(*ctrl));
    if (!ctrl)
        return NULL;

    ctrl->listen_fd = -1;
    snprintf(ctrl->sock_path, sizeof(ctrl->sock_path), "%s", sock_path);

    /* Remove stale socket file. */
    unlink(sock_path);

    ctrl->listen_fd = socket(AF_UNIX, SOCK_STREAM, 0);
    if (ctrl->listen_fd < 0)
        goto fail;

    struct sockaddr_un addr;
    memset(&addr, 0, sizeof(addr));
    addr.sun_family = AF_UNIX;
    snprintf(addr.sun_path, sizeof(addr.sun_path), "%s", sock_path);

    /* Set umask to 0 around bind so the socket is created with 0666
     * atomically — no window where umask-derived restrictive permissions
     * would prevent other daemons from connecting. */
    mode_t old_umask = umask(0);
    int bind_ret = bind(ctrl->listen_fd, (struct sockaddr *)&addr, sizeof(addr));
    umask(old_umask);
    if (bind_ret < 0)
        goto fail;

    if (listen(ctrl->listen_fd, RSS_CTRL_BACKLOG) < 0)
        goto fail;

    return ctrl;

fail:
    if (ctrl->listen_fd >= 0)
        close(ctrl->listen_fd);
    free(ctrl);
    return NULL;
}

void rss_ctrl_destroy(rss_ctrl_t *ctrl)
{
    if (!ctrl)
        return;

    if (ctrl->listen_fd >= 0)
        close(ctrl->listen_fd);

    unlink(ctrl->sock_path);
    free(ctrl);
}

int rss_ctrl_get_fd(rss_ctrl_t *ctrl)
{
    return ctrl ? ctrl->listen_fd : -1;
}

int rss_ctrl_accept_and_handle(rss_ctrl_t *ctrl,
                               int (*handler)(const char *cmd_json, char *resp_buf,
                                              int resp_buf_size, void *userdata),
                               void *userdata)
{
    if (!ctrl || !handler)
        return -EINVAL;

    int client_fd;
    do {
        client_fd = accept(ctrl->listen_fd, NULL, NULL);
    } while (client_fd < 0 && errno == EINTR);

    if (client_fd < 0)
        return -errno;

    /* Set write timeout so a slow/stalled client can't block the daemon
     * indefinitely. Read side is already covered by poll() in read_exact. */
    struct timeval snd_tv = {.tv_sec = 5, .tv_usec = 0};
    (void)setsockopt(client_fd, SOL_SOCKET, SO_SNDTIMEO, &snd_tv, sizeof(snd_tv));

    int ret = -1;
    char *cmd = NULL;

    /* Read the request (5s timeout per poll — a client trickling one byte
     * every 4.9s could hold the handler longer, but on a root-only local
     * Unix socket this is not a practical attack vector). */
    int msg_len = read_message(client_fd, &cmd, 5000);
    if (msg_len < 0) {
        ret = msg_len;
        goto done;
    }

    if (!cmd || msg_len == 0) {
        ret = -EPROTO;
        goto done;
    }

    /* Invoke the handler callback. Heap-allocated — 64KB exceeds what
     * MIPS can allocate in a single stack frame adjustment. */
    char *resp_buf = malloc(RSS_CTRL_MAX_MSG);
    if (!resp_buf) {
        ret = -ENOMEM;
        goto done;
    }
    int resp_len = handler(cmd, resp_buf, RSS_CTRL_MAX_MSG, userdata);

    if (resp_len < 0) {
        /* Handler returned an error -- send a generic error response. */
        resp_len = snprintf(resp_buf, RSS_CTRL_MAX_MSG,
                            "{\"status\":\"error\",\"code\":%d,"
                            "\"msg\":\"handler error\"}",
                            resp_len);
    }

    /* Write the response. */
    ret = write_message(client_fd, resp_buf, (size_t)resp_len);

done:
    free(resp_buf);
    free(cmd);
    close(client_fd);
    return ret;
}

/* ------------------------------------------------------------------ */
/*  Client API (raptorctl side)                                       */
/* ------------------------------------------------------------------ */

int rss_ctrl_send_command(const char *sock_path, const char *cmd_json, char *resp_buf,
                          int resp_buf_size, uint32_t timeout_ms)
{
    if (!sock_path || !cmd_json || !resp_buf || resp_buf_size <= 0)
        return -EINVAL;

    int fd = socket(AF_UNIX, SOCK_STREAM, 0);
    if (fd < 0)
        return -errno;

    struct sockaddr_un addr;
    memset(&addr, 0, sizeof(addr));
    addr.sun_family = AF_UNIX;
    snprintf(addr.sun_path, sizeof(addr.sun_path), "%s", sock_path);

    if (connect(fd, (struct sockaddr *)&addr, sizeof(addr)) < 0) {
        int err = errno;
        close(fd);
        return -err;
    }

    size_t cmd_len = strlen(cmd_json);
    int ret = write_message(fd, cmd_json, cmd_len);
    if (ret < 0) {
        close(fd);
        return ret;
    }

    /* Shutdown write side to signal end of request. */
    shutdown(fd, SHUT_WR);

    /* Read the response. */
    char *resp = NULL;
    int rlen = read_message(fd, &resp, timeout_ms ? timeout_ms : 5000);
    close(fd);

    if (rlen < 0)
        return rlen;

    if (!resp || rlen == 0)
        return -EPROTO;

    /* Copy response into caller's buffer. */
    int copy_len = rlen < resp_buf_size - 1 ? rlen : resp_buf_size - 1;
    memcpy(resp_buf, resp, (size_t)copy_len);
    resp_buf[copy_len] = '\0';
    free(resp);

    return copy_len;
}
