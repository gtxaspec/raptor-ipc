#include "greatest.h"
#include "rss_ipc.h"

#include <poll.h>
#include <pthread.h>
#include <string.h>
#include <unistd.h>
#include <sys/socket.h>
#include <sys/un.h>

#define CTRL_SOCK "/tmp/rss_test_ctrl.sock"

static int echo_handler(const char *cmd_json, char *resp_buf, int resp_buf_size, void *userdata)
{
    (void)userdata;
    return snprintf(resp_buf, resp_buf_size, "{\"echo\":\"%s\"}", cmd_json);
}

/* The listener is non-blocking by contract (a blocking accept() can
 * park a daemon on a spurious event-loop dispatch), so a server calls
 * accept_and_handle only when poll/select reports readability. */
static void serve_one(rss_ctrl_t *ctrl, int (*handler)(const char *, char *, int, void *))
{
    struct pollfd pfd = {.fd = rss_ctrl_get_fd(ctrl), .events = POLLIN};
    if (poll(&pfd, 1, 2000) > 0)
        rss_ctrl_accept_and_handle(ctrl, handler, NULL);
}

static void *server_thread(void *arg)
{
    serve_one(arg, echo_handler);
    return NULL;
}

TEST ctrl_roundtrip(void)
{
    rss_ctrl_t *srv = rss_ctrl_listen(CTRL_SOCK);
    ASSERT(srv);

    pthread_t tid;
    pthread_create(&tid, NULL, server_thread, srv);

    /* Brief pause to let server thread reach accept() */
    usleep(10000);

    char resp[512];
    int ret = rss_ctrl_send_command(CTRL_SOCK, "ping", resp, sizeof(resp), 2000);
    ASSERT(ret > 0);
    ASSERT(strstr(resp, "ping")); /* echoed back */

    pthread_join(tid, NULL);
    rss_ctrl_destroy(srv);
    PASS();
}

static int error_handler(const char *cmd_json, char *resp_buf, int resp_buf_size, void *userdata)
{
    (void)cmd_json;
    (void)resp_buf;
    (void)resp_buf_size;
    (void)userdata;
    return -42; /* negative = error */
}

static void *error_server_thread(void *arg)
{
    serve_one(arg, error_handler);
    return NULL;
}

TEST ctrl_handler_error(void)
{
    const char *sock = "/tmp/rss_test_ctrl_err.sock";
    rss_ctrl_t *srv = rss_ctrl_listen(sock);
    ASSERT(srv);

    pthread_t tid;
    pthread_create(&tid, NULL, error_server_thread, srv);
    usleep(10000);

    char resp[512];
    int ret = rss_ctrl_send_command(sock, "fail", resp, sizeof(resp), 2000);
    ASSERT(ret > 0);
    ASSERT(strstr(resp, "error"));

    pthread_join(tid, NULL);
    rss_ctrl_destroy(srv);
    PASS();
}

static void *stall_thread(void *arg)
{
    int *listen_fd = arg;
    int client = accept(*listen_fd, NULL, NULL);
    if (client >= 0) {
        /* Accept but never respond — just sleep and close */
        usleep(500000);
        close(client);
    }
    return NULL;
}

TEST ctrl_timeout(void)
{
    const char *sock = "/tmp/rss_test_ctrl_to.sock";
    unlink(sock);

    int fd = socket(AF_UNIX, SOCK_STREAM, 0);
    ASSERT(fd >= 0);
    struct sockaddr_un addr;
    memset(&addr, 0, sizeof(addr));
    addr.sun_family = AF_UNIX;
    snprintf(addr.sun_path, sizeof(addr.sun_path), "%s", sock);
    ASSERT_EQ(0, bind(fd, (struct sockaddr *)&addr, sizeof(addr)));
    ASSERT_EQ(0, listen(fd, 1));

    pthread_t tid;
    pthread_create(&tid, NULL, stall_thread, &fd);
    usleep(10000);

    char resp[64];
    int ret = rss_ctrl_send_command(sock, "{}", resp, sizeof(resp), 100);
    /* Should timeout or get connection reset */
    ASSERT(ret < 0);

    pthread_join(tid, NULL);
    close(fd);
    unlink(sock);
    PASS();
}

SUITE(ctrl_suite)
{
    RUN_TEST(ctrl_roundtrip);
    RUN_TEST(ctrl_handler_error);
    RUN_TEST(ctrl_timeout);
}
