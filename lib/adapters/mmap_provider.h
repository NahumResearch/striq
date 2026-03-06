#ifndef STRIQ_MMAP_PROVIDER_H
#define STRIQ_MMAP_PROVIDER_H

/*
 * mmap_provider — block_provider backed by mmap(2).
 *
 * Advantages over file_provider:
 *   - get_block_data: zero allocation, zero syscall — returns pointer into
 *     the already-mapped region.
 *   - release_block:  no-op — no heap memory to free.
 *   - OS page cache handles read-ahead for repeated queries on warm files.
 *
 * Usage:
 *   Call mmap_provider_open() instead of file_provider_init().
 *   Call mmap_provider_close() when done (unmaps the file).
 *
 * Limitations:
 *   - Read-only (PROT_READ | MAP_PRIVATE).
 *   - Files > 2 GB fall back to file_provider on 32-bit systems;
 *     this implementation accepts any size on 64-bit targets.
 *   - POSIX only (mmap, munmap, open, fstat).
 */

#include "../core/block_provider.h"
#include "../core/format/reader.h"
#include <stddef.h>

typedef struct {
    striq_block_provider_t base;   /* MUST be first — cast-compatible */
    striq_fmt_reader_t    *fmt;
    void                  *map;    /* mmap'd region */
    size_t                 map_len;
    int                    fd;
} striq_mmap_provider_t;

/*
 * Open the file, mmap it, parse the footer into *fmt (must be allocated by
 * caller), and initialise the provider vtable.
 *
 * On success, the caller must eventually call mmap_provider_close().
 */
striq_status_t mmap_provider_open(
    striq_mmap_provider_t *p,
    striq_fmt_reader_t    *fmt,
    const char            *path);

/*
 * Unmap the file and close the file descriptor.
 * Safe to call if mmap_provider_open() failed partway through.
 */
void mmap_provider_close(striq_mmap_provider_t *p);

#endif /* STRIQ_MMAP_PROVIDER_H */
