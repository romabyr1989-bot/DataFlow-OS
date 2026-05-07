#pragma once
#include <stdio.h>
#include <time.h>
#include <pthread.h>

typedef enum { LOG_DEBUG=0, LOG_INFO, LOG_WARN, LOG_ERROR } LogLevel;

typedef struct {
    FILE     *out;
    LogLevel  min_level;
    int       json_mode;
    pthread_mutex_t mu;
} Logger;

extern Logger g_log;

/* Per-thread correlation ID (UUID4: 36 chars + NUL).
 * Automatically included in JSON log output.
 * Set once per HTTP request by the HTTP layer. */
extern _Thread_local char g_correlation_id[37];

void log_init(Logger *l, FILE *out, LogLevel min_level, int json_mode);
void log_write(Logger *l, LogLevel level, const char *file, int line,
               const char *fmt, ...) __attribute__((format(printf,5,6)));

/* Copy id (up to 36 chars) into g_correlation_id for current thread */
void log_set_correlation_id(const char *id);

/* Generate a new UUID4 and store in g_correlation_id for current thread */
void log_new_correlation_id(void);

#define LOG_DEBUG(...) log_write(&g_log, LOG_DEBUG, __FILE__, __LINE__, __VA_ARGS__)
#define LOG_INFO(...)  log_write(&g_log, LOG_INFO,  __FILE__, __LINE__, __VA_ARGS__)
#define LOG_WARN(...)  log_write(&g_log, LOG_WARN,  __FILE__, __LINE__, __VA_ARGS__)
#define LOG_ERROR(...) log_write(&g_log, LOG_ERROR, __FILE__, __LINE__, __VA_ARGS__)
