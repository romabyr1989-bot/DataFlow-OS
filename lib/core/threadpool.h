#pragma once
#include <stddef.h>
#include <pthread.h>

typedef void (*task_fn)(void *arg);

typedef struct Task { task_fn fn; void *arg; struct Task *next; } Task;

typedef struct {
    pthread_t       *threads;
    int              nthreads;
    Task            *head, *tail;
    int              queue_size, queue_max;
    pthread_mutex_t  mu;
    pthread_cond_t   cond_work;
    pthread_cond_t   cond_idle;
    int              shutdown;
    int              active;
} ThreadPool;

ThreadPool *tp_create(int nthreads, int queue_max);
void        tp_submit(ThreadPool *p, task_fn fn, void *arg); /* blocks if full */
void        tp_wait(ThreadPool *p);
void        tp_destroy(ThreadPool *p);
int         tp_active(ThreadPool *p);
