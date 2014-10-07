#ifndef BUFFER_H
#define BUFFER_H

#include <stdlib.h>
#include <stdint.h>
#include <stdbool.h>
#include <apr_network_io.h>

#define SZ_BUF (1*1024*1024) // 1M

typedef struct {
    uint8_t *raw;
    size_t sz;
    size_t idx_read;
    size_t idx_write;
} buf_t;

void buf_create(buf_t **buf);

void buf_destroy(buf_t *buf);

size_t buf_readjust(buf_t *buf, size_t sz);

size_t buf_realloc(buf_t *buf, size_t sz);

size_t buf_write(buf_t *buf, uint8_t *data, size_t sz);

size_t buf_read(buf_t *buf, uint8_t *data, size_t sz);

size_t buf_peek(buf_t *buf, uint8_t *data, size_t sz);

apr_status_t buf_to_sock(buf_t *buf, apr_socket_t *sock);

apr_status_t buf_from_sock(buf_t *buf, apr_socket_t *sock);

static size_t buf_sz_content(buf_t *buf) {
    return buf->idx_write - buf->idx_read;
}

static size_t buf_sz_cnt(buf_t *buf) {
    return buf->idx_write - buf->idx_read;
}

#endif // BUFFER_H
