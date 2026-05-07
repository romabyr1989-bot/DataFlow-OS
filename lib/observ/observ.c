#include "observ.h"
#include <math.h>
#include <string.h>
#include <stdio.h>
#include <time.h>

void metrics_init(Metrics *m) {
    memset(m, 0, sizeof(Metrics));
    pthread_mutex_init(&m->rows_ingested.mu, NULL);
    pthread_mutex_init(&m->rows_failed.mu, NULL);
    pthread_mutex_init(&m->pipeline_latency_ms.mu, NULL);
    pthread_mutex_init(&m->query_latency_ms.mu, NULL);
    pthread_mutex_init(&m->http_request_duration_ms.mu, NULL);
    m->uptime_start = (int64_t)time(NULL);
}

void metrics_push(MetricRing *r, double val) {
    pthread_mutex_lock(&r->mu);
    r->values[r->head]     = val;
    r->timestamps[r->head] = (int64_t)time(NULL);
    r->head = (r->head + 1) % METRIC_RING_SIZE;
    if (r->count < METRIC_RING_SIZE) r->count++;
    pthread_mutex_unlock(&r->mu);
}

double metrics_avg(MetricRing *r, int n) {
    pthread_mutex_lock(&r->mu);
    int take = n < r->count ? n : r->count;
    if (!take) { pthread_mutex_unlock(&r->mu); return 0.0; }
    double sum = 0;
    int pos = (r->head - take + METRIC_RING_SIZE) % METRIC_RING_SIZE;
    for (int i = 0; i < take; i++) {
        sum += r->values[(pos + i) % METRIC_RING_SIZE];
    }
    pthread_mutex_unlock(&r->mu);
    return sum / take;
}

char *metrics_to_json(Metrics *m, Arena *a) {
    int64_t now = (int64_t)time(NULL);
    char *buf = arena_sprintf(a,
        "{\"uptime\":%lld,"
        "\"total_rows\":%lld,"
        "\"total_pipelines_run\":%lld,"
        "\"total_queries\":%lld,"
        "\"avg_ingest_rows_1min\":%.1f,"
        "\"avg_query_latency_ms\":%.1f,"
        "\"avg_pipeline_latency_ms\":%.1f}",
        (long long)(now - m->uptime_start),
        (long long)m->total_rows,
        (long long)m->total_pipelines_run,
        (long long)m->total_queries,
        metrics_avg(&m->rows_ingested, 60),
        metrics_avg(&m->query_latency_ms, 60),
        metrics_avg(&m->pipeline_latency_ms, 60));
    return buf;
}

int qc_run(QualityRule *rule, const char *table_name,
           int64_t row_count, Arena *a, char **report_out) {
    char *buf = arena_alloc(a, 4096);
    int off = 0;
    off += snprintf(buf+off, 4096-off,
                    "{\"table\":\"%s\",\"rows\":%lld,\"checks\":[",
                    table_name, (long long)row_count);
    int total_fails = 0;
    for (int i = 0; i < rule->nchecks; i++) {
        QualityCheck *qc = &rule->checks[i];
        /* simplified: just report status */
        bool pass = (qc->fail_count == 0);
        if (!pass) total_fails++;
        off += snprintf(buf+off, 4096-off,
                        "%s{\"column\":\"%s\",\"type\":%d,\"pass\":%s,"
                        "\"pass_count\":%d,\"fail_count\":%d}",
                        i?",":"", qc->column, (int)qc->type,
                        pass?"true":"false", qc->pass_count, qc->fail_count);
    }
    off += snprintf(buf+off, 4096-off, "],\"total_fails\":%d}", total_fails);
    *report_out = buf;
    return total_fails;
}

void anomaly_init(AnomalyDetector *d, double threshold) {
    memset(d, 0, sizeof(*d));
    d->threshold = threshold > 0 ? threshold : 3.0;
}

bool anomaly_check(AnomalyDetector *d, double val) {
    double z = fabs(anomaly_zscore(d, val));
    /* update window */
    d->window[d->head] = val;
    d->head = (d->head + 1) % 128;
    if (d->n < 128) d->n++;
    return z > d->threshold;
}

double anomaly_zscore(AnomalyDetector *d, double val) {
    if (d->n < 2) return 0.0;
    double sum = 0, sq = 0;
    for (int i = 0; i < d->n; i++) { sum += d->window[i]; sq += d->window[i]*d->window[i]; }
    double mean = sum / d->n;
    double var  = sq/d->n - mean*mean;
    if (var < 0.0) var = 0.0; /* guard against floating-point rounding */
    double sd   = var > 1e-10 ? sqrt(var) : 1e-10;
    return (val - mean) / sd;
}
