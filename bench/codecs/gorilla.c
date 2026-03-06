#include "gorilla.h"
#include <string.h>
#include <stdint.h>

/*
 * Gorilla XOR encoding (VLDB 2015, §4.1).
 *
 * Layout of output stream:
 *   [8 bytes]  first value raw (IEEE-754 bit pattern)
 *   [N-1 blocks]  each encoded as described below
 *
 * Per-value encoding:
 *   '0' bit  → value == prev_value (skip)
 *   '1','0'  → XOR fits within previous [leading, trailing] window
 *   '1','1'  → new window: 5-bit leading count, 6-bit meaningful length - 1, then payload
 */

typedef struct {
    uint8_t *buf;
    size_t   cap;       /* bytes */
    size_t   bit_pos;   /* current write position in bits */
} bs_writer_t;

typedef struct {
    const uint8_t *buf;
    size_t         data_size; /* bytes */
    size_t         bit_pos;
} bs_reader_t;

static inline int bs_write_bit(bs_writer_t *w, int bit)
{
    size_t byte_idx = w->bit_pos >> 3;
    if (byte_idx >= w->cap) return -1;
    if ((w->bit_pos & 7) == 0) w->buf[byte_idx] = 0;
    if (bit) w->buf[byte_idx] |= (uint8_t)(0x80u >> (w->bit_pos & 7));
    w->bit_pos++;
    return 0;
}

static inline int bs_write_bits(bs_writer_t *w, uint64_t val, int nbits)
{
    for (int i = nbits - 1; i >= 0; i--)
        if (bs_write_bit(w, (int)((val >> i) & 1u)) < 0) return -1;
    return 0;
}

static inline int bs_read_bit(bs_reader_t *r)
{
    size_t byte_idx = r->bit_pos >> 3;
    if (byte_idx >= r->data_size) return -1;
    int bit = (r->buf[byte_idx] >> (7 - (r->bit_pos & 7))) & 1;
    r->bit_pos++;
    return bit;
}

static inline int64_t bs_read_bits(bs_reader_t *r, int nbits)
{
    uint64_t val = 0;
    for (int i = 0; i < nbits; i++) {
        int b = bs_read_bit(r);
        if (b < 0) return -1;
        val = (val << 1) | (uint64_t)b;
    }
    return (int64_t)val;
}

static inline int count_leading_zeros_64(uint64_t x)
{
    if (x == 0) return 64;
#if defined(__GNUC__) || defined(__clang__)
    return __builtin_clzll(x);
#else
    int n = 0;
    while (!(x & (UINT64_C(1) << 63))) { n++; x <<= 1; }
    return n;
#endif
}

static inline int count_trailing_zeros_64(uint64_t x)
{
    if (x == 0) return 64;
#if defined(__GNUC__) || defined(__clang__)
    return __builtin_ctzll(x);
#else
    int n = 0;
    while (!(x & 1u)) { n++; x >>= 1; }
    return n;
#endif
}

static inline uint64_t d2b(double d)
{
    uint64_t u; memcpy(&u, &d, 8); return u;
}
static inline double b2d(uint64_t u)
{
    double d; memcpy(&d, &u, 8); return d;
}

size_t gorilla_compress(const double *values, size_t count,
                        uint8_t *out, size_t cap)
{
    if (!values || !out || count == 0 || cap < 8) return 0;

    bs_writer_t w = { out, cap, 0 };

    uint64_t first = d2b(values[0]);
    for (int i = 63; i >= 0; i--)
        if (bs_write_bit(&w, (int)((first >> i) & 1u)) < 0) return 0;

    int prev_leading  = 0xFF; /* sentinel: force new window on first XOR */
    int prev_trailing = 0;
    uint64_t prev_val = first;

    for (size_t idx = 1; idx < count; idx++) {
        uint64_t cur = d2b(values[idx]);
        uint64_t xor_val = cur ^ prev_val;

        if (xor_val == 0) {
            if (bs_write_bit(&w, 0) < 0) return 0;
        } else {
            if (bs_write_bit(&w, 1) < 0) return 0;

            int leading  = count_leading_zeros_64(xor_val);
            int trailing = count_trailing_zeros_64(xor_val);

            /* Clamp leading to 5-bit max (31) */
            if (leading  > 31) leading  = 31;

            int meaningful = 64 - leading - trailing;

            if (prev_leading != 0xFF &&
                leading  >= prev_leading &&
                trailing >= prev_trailing)
            {
                /* Fits in previous window */
                if (bs_write_bit(&w, 0) < 0) return 0;
                int prev_meaningful = 64 - prev_leading - prev_trailing;
                if (bs_write_bits(&w, xor_val >> prev_trailing, prev_meaningful) < 0)
                    return 0;
            } else {
                /* New window */
                if (bs_write_bit(&w, 1) < 0) return 0;
                if (bs_write_bits(&w, (uint64_t)leading,      5)  < 0) return 0;
                if (bs_write_bits(&w, (uint64_t)(meaningful - 1), 6)  < 0) return 0;
                if (bs_write_bits(&w, xor_val >> trailing, meaningful) < 0) return 0;
                prev_leading  = leading;
                prev_trailing = trailing;
            }
        }
        prev_val = cur;
    }

    /* Return bytes used (ceiling) */
    return (w.bit_pos + 7) >> 3;
}

size_t gorilla_decompress(const uint8_t *data, size_t data_size,
                          size_t expected_count,
                          double *out, size_t out_cap)
{
    if (!data || !out || expected_count == 0 || out_cap < expected_count) return 0;

    bs_reader_t r = { data, data_size, 0 };

    int64_t raw = bs_read_bits(&r, 64);
    if (raw < 0) return 0;
    out[0] = b2d((uint64_t)raw);

    int prev_leading  = 0;
    int prev_trailing = 0;
    uint64_t prev_val = (uint64_t)raw;

    for (size_t idx = 1; idx < expected_count; idx++) {
        int b0 = bs_read_bit(&r);
        if (b0 < 0) return idx;

        if (b0 == 0) {
            out[idx] = b2d(prev_val);
        } else {
            int b1 = bs_read_bit(&r);
            if (b1 < 0) return idx;

            if (b1 == 0) {
                /* Use previous window */
                int prev_meaningful = 64 - prev_leading - prev_trailing;
                int64_t payload = bs_read_bits(&r, prev_meaningful);
                if (payload < 0) return idx;
                uint64_t xor_val = (uint64_t)payload << prev_trailing;
                prev_val ^= xor_val;
            } else {
                /* New window */
                int64_t lead = bs_read_bits(&r, 5);
                int64_t mlen = bs_read_bits(&r, 6);
                if (lead < 0 || mlen < 0) return idx;
                int meaningful = (int)mlen + 1;
                int64_t payload = bs_read_bits(&r, meaningful);
                if (payload < 0) return idx;
                prev_leading  = (int)lead;
                prev_trailing = 64 - prev_leading - meaningful;
                uint64_t xor_val = (uint64_t)payload << prev_trailing;
                prev_val ^= xor_val;
            }
            out[idx] = b2d(prev_val);
        }
    }

    return expected_count;
}
