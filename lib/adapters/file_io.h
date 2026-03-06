#ifndef STRIQ_FILE_IO_H
#define STRIQ_FILE_IO_H

/*
 * File I/O adapter — the ONLY place in the project that touches
 * the OS filesystem. Provides striq_write_fn / striq_read_fn /
 * striq_seek_fn implementations backed by FILE*.
 */

#include "../core/types.h"
#include <stdio.h>

typedef struct {
    FILE *fp;
} striq_file_ctx_t;

striq_status_t file_io_write(const uint8_t *data, size_t len, void *ctx);
striq_status_t file_io_read(uint8_t *buf, size_t len, void *ctx);
striq_status_t file_io_seek(int64_t offset, int whence, void *ctx);

/* Returns the file size in bytes, or 0 on error. */
uint64_t file_io_size(FILE *fp);

#endif /* STRIQ_FILE_IO_H */
