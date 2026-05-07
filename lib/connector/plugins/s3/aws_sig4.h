#pragma once
#include "../../../core/arena.h"
#include <stddef.h>

typedef struct {
    char access_key[128];
    char secret_key[128];
    char region[32];
    char service[16];
} AwsCredentials;

/* Fills auth_header_out and date_header_out.
   auth_header_out needs a buffer of ~512 bytes.
   date_header_out needs a buffer of ~20 bytes (ISO8601 compact).
   payload_sha256 = hex-SHA256 of request body ("e3b0..." for empty body).
   Returns 0 on success. */
int aws_sig4_sign(const AwsCredentials *creds,
                  const char *method, const char *host, const char *path,
                  const char *query_string, const char *payload_sha256,
                  char *auth_header_out, size_t auth_len,
                  char *date_header_out, size_t date_len,
                  Arena *a);
