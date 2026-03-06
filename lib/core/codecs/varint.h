#ifndef STRIQ_VARINT_H
#define STRIQ_VARINT_H

#include <stdint.h>
#include <stddef.h>

static inline size_t varint_write_u64(uint64_t val, uint8_t *out, size_t cap)
{
    size_t i = 0;
    while (val >= 0x80u) {
        if (i >= cap) return 0;
        out[i++] = (uint8_t)((val & 0x7Fu) | 0x80u);
        val >>= 7;
    }
    if (i >= cap) return 0;
    out[i++] = (uint8_t)(val & 0x7Fu);
    return i;
}

static inline size_t varint_read_u64(const uint8_t *in, size_t in_len, uint64_t *out)
{
    uint64_t val = 0;
    uint32_t shift = 0;
    size_t i = 0;
    while (i < in_len) {
        uint8_t b = in[i++];
        val |= (uint64_t)(b & 0x7Fu) << shift;
        if (!(b & 0x80u)) { *out = val; return i; }
        shift += 7;
        if (shift >= 64) return 0;
    }
    return 0;
}

static inline size_t varint_write_u32(uint8_t *dst, uint32_t val)
{
    size_t n = 0;
    do {
        uint8_t byte = (uint8_t)(val & 0x7Fu);
        val >>= 7;
        if (val) byte |= 0x80u;
        dst[n++] = byte;
    } while (val);
    return n;
}

static inline size_t varint_read_u32(const uint8_t *src, size_t src_len,
                                     size_t offset, uint32_t *out)
{
    uint32_t val = 0;
    int shift = 0;
    size_t n = 0;
    while (offset + n < src_len && shift < 35) {
        uint8_t b = src[offset + n++];
        val |= (uint32_t)(b & 0x7Fu) << shift;
        shift += 7;
        if (!(b & 0x80u)) { *out = val; return n; }
    }
    return 0;
}

#endif /* STRIQ_VARINT_H */
