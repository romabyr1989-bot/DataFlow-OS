#include <openssl/hmac.h>
#include <openssl/sha.h>
#include <time.h>
#include <string.h>
#include <stdio.h>
#include "../../../core/arena.h"
#include "aws_sig4.h"

/* ── helpers ── */

static void hex_encode(const unsigned char *src, size_t src_len,
                       char *dst, size_t dst_cap)
{
    static const char hex[] = "0123456789abcdef";
    size_t i;
    for (i = 0; i < src_len && (i * 2 + 2) < dst_cap; i++) {
        dst[i * 2]     = hex[(src[i] >> 4) & 0xf];
        dst[i * 2 + 1] = hex[src[i] & 0xf];
    }
    dst[i * 2] = '\0';
}

/* SHA-256 of data, result as lowercase hex string written to out (65 bytes). */
static void sha256_hex(const char *data, size_t len, char *out)
{
    unsigned char digest[SHA256_DIGEST_LENGTH];
    SHA256((const unsigned char *)data, len, digest);
    hex_encode(digest, SHA256_DIGEST_LENGTH, out, 65);
}

/* HMAC-SHA256: result written to out_digest (32 bytes), returns out_digest. */
static unsigned char *hmac_sha256(const unsigned char *key, size_t key_len,
                                   const char *data, size_t data_len,
                                   unsigned char *out_digest)
{
    unsigned int out_len = 0;
    HMAC(EVP_sha256(), key, (int)key_len,
         (const unsigned char *)data, data_len,
         out_digest, &out_len);
    return out_digest;
}

/* ── public API ── */

int aws_sig4_sign(const AwsCredentials *creds,
                  const char *method, const char *host, const char *path,
                  const char *query_string, const char *payload_sha256,
                  char *auth_header_out, size_t auth_len,
                  char *date_header_out, size_t date_len,
                  Arena *a)
{
    (void)a; /* arena available for future use */

    /* 1. Date/datetime strings */
    time_t now = time(NULL);
    struct tm tm_utc;
    gmtime_r(&now, &tm_utc);

    char date[9];     /* YYYYMMDD */
    char datetime[17]; /* YYYYMMDDTHHmmssZ */
    strftime(date, sizeof(date), "%Y%m%d", &tm_utc);
    strftime(datetime, sizeof(datetime), "%Y%m%dT%H%M%SZ", &tm_utc);

    /* Write datetime to caller's date_header_out */
    snprintf(date_header_out, date_len, "%s", datetime);

    /* 2. Canonical request
       Headers must be sorted alphabetically: host < x-amz-date */
    char canonical_headers[512];
    snprintf(canonical_headers, sizeof(canonical_headers),
             "host:%s\nx-amz-date:%s\n", host, datetime);
    const char *signed_headers = "host;x-amz-date";

    char cr_hash[65]; /* SHA-256 hex of canonical request */
    {
        char canonical_request[2048];
        snprintf(canonical_request, sizeof(canonical_request),
                 "%s\n%s\n%s\n%s\n%s\n%s",
                 method,
                 path,
                 query_string ? query_string : "",
                 canonical_headers,
                 signed_headers,
                 payload_sha256);
        sha256_hex(canonical_request, strlen(canonical_request), cr_hash);
    }

    /* 3. Credential scope */
    char scope[128];
    snprintf(scope, sizeof(scope), "%s/%s/%s/aws4_request",
             date, creds->region, creds->service);

    /* 4. String to sign */
    char string_to_sign[1024];
    snprintf(string_to_sign, sizeof(string_to_sign),
             "AWS4-HMAC-SHA256\n%s\n%s\n%s",
             datetime, scope, cr_hash);

    /* 5. Signing key:
       kSecret  = "AWS4" + secret_key
       kDate    = HMAC(kSecret,   date)
       kRegion  = HMAC(kDate,     region)
       kService = HMAC(kRegion,   service)
       kSigning = HMAC(kService,  "aws4_request") */
    unsigned char k_secret[140];
    size_t k_secret_len = 4 + strlen(creds->secret_key);
    memcpy(k_secret, "AWS4", 4);
    memcpy(k_secret + 4, creds->secret_key, strlen(creds->secret_key));

    unsigned char k_date[32], k_region[32], k_service[32], k_signing[32];
    hmac_sha256(k_secret,   k_secret_len,  date,           strlen(date),                k_date);
    hmac_sha256(k_date,     32,            creds->region,  strlen(creds->region),        k_region);
    hmac_sha256(k_region,   32,            creds->service, strlen(creds->service),       k_service);
    hmac_sha256(k_service,  32,            "aws4_request", strlen("aws4_request"),       k_signing);

    /* 6. Signature */
    unsigned char sig_bytes[32];
    hmac_sha256(k_signing, 32, string_to_sign, strlen(string_to_sign), sig_bytes);

    char signature[65];
    hex_encode(sig_bytes, 32, signature, sizeof(signature));

    /* 7. Authorization header */
    snprintf(auth_header_out, auth_len,
             "AWS4-HMAC-SHA256 Credential=%s/%s,SignedHeaders=%s,Signature=%s",
             creds->access_key, scope, signed_headers, signature);

    return 0;
}
