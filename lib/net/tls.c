#include "tls.h"
#include "../core/log.h"
#include <openssl/ssl.h>
#include <openssl/err.h>
#include <stdlib.h>
#include <string.h>

typedef struct TlsCtx {
    SSL_CTX *ssl_ctx;
} TlsCtx;

typedef struct TlsConn {
    SSL *ssl;
    int fd;
} TlsConn;

TlsCtx *tls_server_ctx_create(const char *cert_pem_path,
                               const char *key_pem_path) {
    if (!cert_pem_path || !key_pem_path) {
        LOG_ERROR("tls: cert and key paths required");
        return NULL;
    }

    /* Initialize SSL library */
    SSL_library_init();
    SSL_load_error_strings();

    /* Create context */
    SSL_CTX *ctx = SSL_CTX_new(TLS_server_method());
    if (!ctx) {
        LOG_ERROR("tls: SSL_CTX_new failed");
        return NULL;
    }

    /* Set minimum TLS version to 1.2 */
    if (!SSL_CTX_set_min_proto_version(ctx, TLS1_2_VERSION)) {
        LOG_ERROR("tls: failed to set min TLS version");
        SSL_CTX_free(ctx);
        return NULL;
    }

    /* Load certificate */
    if (SSL_CTX_use_certificate_file(ctx, cert_pem_path, SSL_FILETYPE_PEM) <= 0) {
        LOG_ERROR("tls: failed to load certificate from %s", cert_pem_path);
        ERR_print_errors_fp(stderr);
        SSL_CTX_free(ctx);
        return NULL;
    }

    /* Load private key */
    if (SSL_CTX_use_PrivateKey_file(ctx, key_pem_path, SSL_FILETYPE_PEM) <= 0) {
        LOG_ERROR("tls: failed to load private key from %s", key_pem_path);
        ERR_print_errors_fp(stderr);
        SSL_CTX_free(ctx);
        return NULL;
    }

    /* Verify private key and certificate match */
    if (!SSL_CTX_check_private_key(ctx)) {
        LOG_ERROR("tls: certificate and private key do not match");
        SSL_CTX_free(ctx);
        return NULL;
    }

    /* Set cipher suite: disable weak ciphers */
    if (!SSL_CTX_set_cipher_list(ctx, "HIGH:!aNULL:!eNULL:!EXPORT:!RC4")) {
        LOG_WARN("tls: failed to set cipher list");
    }

    TlsCtx *tctx = malloc(sizeof(TlsCtx));
    if (!tctx) {
        SSL_CTX_free(ctx);
        return NULL;
    }
    tctx->ssl_ctx = ctx;
    LOG_INFO("tls: server context created for %s", cert_pem_path);
    return tctx;
}

void tls_server_ctx_destroy(TlsCtx *ctx) {
    if (!ctx) return;
    SSL_CTX_free(ctx->ssl_ctx);
    free(ctx);
}

TlsConn *tls_conn_accept(TlsCtx *ctx, int fd) {
    if (!ctx) return NULL;

    SSL *ssl = SSL_new(ctx->ssl_ctx);
    if (!ssl) {
        LOG_ERROR("tls: SSL_new failed");
        return NULL;
    }

    if (!SSL_set_fd(ssl, fd)) {
        LOG_ERROR("tls: SSL_set_fd failed");
        SSL_free(ssl);
        return NULL;
    }

    int accept_result = SSL_accept(ssl);
    if (accept_result <= 0) {
        int ssl_err = SSL_get_error(ssl, accept_result);
        if (ssl_err != SSL_ERROR_WANT_READ && ssl_err != SSL_ERROR_WANT_WRITE) {
            LOG_WARN("tls: SSL_accept failed with error %d", ssl_err);
        }
        SSL_free(ssl);
        return NULL;
    }

    TlsConn *conn = malloc(sizeof(TlsConn));
    if (!conn) {
        SSL_free(ssl);
        return NULL;
    }
    conn->ssl = ssl;
    conn->fd = fd;
    return conn;
}

void tls_conn_destroy(TlsConn *conn) {
    if (!conn) return;
    if (conn->ssl) {
        SSL_shutdown(conn->ssl);
        SSL_free(conn->ssl);
    }
    free(conn);
}

ssize_t tls_read(TlsConn *conn, void *buf, size_t n) {
    if (!conn) return -1;
    int ret = SSL_read(conn->ssl, buf, (int)n);
    if (ret > 0) return ret;
    int ssl_err = SSL_get_error(conn->ssl, ret);
    if (ssl_err == SSL_ERROR_WANT_READ || ssl_err == SSL_ERROR_WANT_WRITE)
        return 0;  /* Non-blocking would block */
    return -1;
}

ssize_t tls_write(TlsConn *conn, const void *buf, size_t n) {
    if (!conn) return -1;
    int ret = SSL_write(conn->ssl, buf, (int)n);
    if (ret > 0) return ret;
    int ssl_err = SSL_get_error(conn->ssl, ret);
    if (ssl_err == SSL_ERROR_WANT_READ || ssl_err == SSL_ERROR_WANT_WRITE)
        return 0;  /* Non-blocking would block */
    return -1;
}

int tls_conn_fd(TlsConn *conn) {
    if (!conn) return -1;
    return conn->fd;
}
