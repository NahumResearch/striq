#ifndef STRIQ_ARENA_H
#define STRIQ_ARENA_H

#include "types.h"
#include <string.h>

/* Initialise arena over a caller-owned buffer. */
static inline striq_status_t arena_init(striq_arena_t *a, uint8_t *buf, size_t cap)
{
    if (!a || !buf || cap == 0) return STRIQ_ERR_PARAM;
    a->buf  = buf;
    a->cap  = cap;
    a->used = 0;
    return STRIQ_OK;
}

/* Allocate n bytes (aligned to 8 bytes). Returns NULL on overflow. */
static inline void *arena_alloc(striq_arena_t *a, size_t n)
{
    /* align up to 8 */
    size_t aligned = (n + 7u) & ~(size_t)7u;
    if (a->used + aligned > a->cap) return NULL;
    void *ptr = a->buf + a->used;
    a->used += aligned;
    return ptr;
}

/* Reset without freeing the backing buffer. */
static inline void arena_reset(striq_arena_t *a)
{
    a->used = 0;
}

#endif /* STRIQ_ARENA_H */
