/*
 * rss_ipc_log.c -- IPC library logging.
 *
 * Default: fprintf(stderr). Daemons override via rss_ipc_set_log()
 * to route through raptor-common's leveled logger.
 */

#include "rss_ipc.h"

#include <stdarg.h>
#include <stdio.h>

static const char *level_str(int level)
{
    switch (level) {
    case RSS_IPC_LOG_ERROR:
        return "ERROR";
    case RSS_IPC_LOG_WARN:
        return "WARN ";
    case RSS_IPC_LOG_INFO:
        return "INFO ";
    case RSS_IPC_LOG_DEBUG:
        return "DEBUG";
    default:
        return "?????";
    }
}

static void default_log(int level, const char *file, int line, const char *fmt, ...)
{
    va_list ap;
    va_start(ap, fmt);
    fprintf(stderr, "[%s] %s:%d: ", level_str(level), file, line);
    vfprintf(stderr, fmt, ap);
    fputc('\n', stderr);
    va_end(ap);
}

static rss_ipc_log_fn log_fn = default_log;

void rss_ipc_set_log(rss_ipc_log_fn fn)
{
    log_fn = fn ? fn : default_log;
}

void rss_ipc_log(int level, const char *file, int line, const char *fmt, ...)
{
    va_list ap;
    va_start(ap, fmt);

    char buf[512];
    vsnprintf(buf, sizeof(buf), fmt, ap);
    va_end(ap);

    log_fn(level, file, line, "%s", buf);
}
