#ifndef STRIQ_ENDIAN_H
#define STRIQ_ENDIAN_H

#include <stdint.h>

static inline uint32_t read_u32_le(const uint8_t *b)
{
    return (uint32_t)b[0] | ((uint32_t)b[1] << 8) |
           ((uint32_t)b[2] << 16) | ((uint32_t)b[3] << 24);
}

#endif /* STRIQ_ENDIAN_H */
