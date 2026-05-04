/* Потокобезопасный логгер с поддержкой двух режимов вывода:
 * - text (цветной ANSI) — для разработки/просмотра в терминале
 * - json — для передачи в Loki/ELK/Vector без дополнительного парсинга */
#include "log.h"
#include <stdarg.h>
#include <string.h>

Logger g_log;

static const char *level_str[]   = {"DEBUG","INFO","WARN","ERROR"};
static const char *level_color[] = {"\033[37m","\033[32m","\033[33m","\033[31m"};

void log_init(Logger *l, FILE *out, LogLevel min_level, int json_mode) {
    l->out = out; l->min_level = min_level; l->json_mode = json_mode;
    pthread_mutex_init(&l->mu, NULL);
}

void log_write(Logger *l, LogLevel level, const char *file, int line,
               const char *fmt, ...) {
    if (level < l->min_level) return;
    struct timespec ts; clock_gettime(CLOCK_REALTIME, &ts);
    struct tm tm; gmtime_r(&ts.tv_sec, &tm);
    char tbuf[32];
    strftime(tbuf, sizeof(tbuf), "%Y-%m-%dT%H:%M:%S", &tm);

    va_list ap; va_start(ap, fmt);
    char msg[4096]; vsnprintf(msg, sizeof(msg), fmt, ap); va_end(ap);

    /* Убираем путь к файлу, оставляем только имя — лаконичнее в логах. */
    const char *f = strrchr(file, '/'); f = f ? f+1 : file;

    /* Мьютекс обеспечивает атомарность строки: строки от разных потоков не перемежаются. */
    pthread_mutex_lock(&l->mu);
    if (l->json_mode) {
        fprintf(l->out, "{\"ts\":\"%s.%03ldZ\",\"level\":\"%s\","
                "\"file\":\"%s\",\"line\":%d,\"msg\":\"%s\"}\n",
                tbuf, ts.tv_nsec/1000000, level_str[level], f, line, msg);
    } else {
        fprintf(l->out, "%s%s.%03ldZ [%5s] %s:%d — %s\033[0m\n",
                level_color[level], tbuf, ts.tv_nsec/1000000,
                level_str[level], f, line, msg);
    }
    fflush(l->out);   /* немедленный сброс — при краше теряем меньше строк */
    pthread_mutex_unlock(&l->mu);
}
