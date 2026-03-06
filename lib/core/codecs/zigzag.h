#ifndef STRIQ_ZIGZAG_H
#define STRIQ_ZIGZAG_H

#include <stdint.h>

static inline uint64_t zigzag_encode(int64_t n)
{
    return ((uint64_t)n << 1) ^ (uint64_t)(n >> 63);
}

static inline int64_t zigzag_decode(uint64_t n)
{
    return (int64_t)((n >> 1) ^ (uint64_t)(-(int64_t)(n & 1u)));
}

#endif /* STRIQ_ZIGZAG_H */
