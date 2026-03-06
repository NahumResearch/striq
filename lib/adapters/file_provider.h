#ifndef STRIQ_FILE_PROVIDER_H
#define STRIQ_FILE_PROVIDER_H

/*
 * file_provider — block_provider implementation backed by striq_fmt_reader_t.
 *
 * get_block_data() mallocs a buffer and reads from disk.
 * release_block()  frees the buffer.
 */

#include "../core/block_provider.h"
#include "../core/format/reader.h"

typedef struct {
    striq_block_provider_t base;   /* MUST be first — cast-compatible */
    striq_fmt_reader_t    *fmt;
} striq_file_provider_t;

striq_status_t file_provider_init(
    striq_file_provider_t *p,
    striq_fmt_reader_t    *fmt);

#endif /* STRIQ_FILE_PROVIDER_H */
