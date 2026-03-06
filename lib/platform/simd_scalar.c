#include "simd.h"
#include <string.h>

void striq_byte_shuffle(const uint8_t *src, uint8_t *dst,
                        size_t count, size_t elem_size)
{
    for (size_t i = 0; i < count; i++)
        for (size_t b = 0; b < elem_size; b++)
            dst[b * count + i] = src[i * elem_size + b];
}

void striq_byte_unshuffle(const uint8_t *src, uint8_t *dst,
                          size_t count, size_t elem_size)
{
    for (size_t i = 0; i < count; i++)
        for (size_t b = 0; b < elem_size; b++)
            dst[i * elem_size + b] = src[b * count + i];
}

void striq_cheb_eval_batch(const double c[4], size_t length, double *out)
{
    if (length == 0) return;
    if (length == 1) { out[0] = c[0]; return; }

    for (size_t i = 0; i < length; i++) {
        double u = -1.0 + 2.0 * (double)i / (double)(length - 1);
        double b3 = c[3];
        double b2 = c[2] + 2.0 * u * b3;
        double b1 = c[1] + 2.0 * u * b2 - b3;
        out[i]    = c[0] + u * b1 - b2;
    }
}

void striq_stats_reduce(
    const double *data, size_t n,
    double *out_sum, double *out_min, double *out_max,
    double *out_sum_sq, uint64_t *out_nz_count)
{
    if (n == 0) {
        *out_sum = 0.0; *out_min = 0.0; *out_max = 0.0;
        *out_sum_sq = 0.0; *out_nz_count = 0;
        return;
    }
    double sum = 0.0, min_v = data[0], max_v = data[0], sum_sq = 0.0;
    uint64_t nz = 0;
    for (size_t i = 0; i < n; i++) {
        double v = data[i];
        sum    += v;
        sum_sq += v * v;
        if (v < min_v) min_v = v;
        if (v > max_v) max_v = v;
        if (v != 0.0) nz++;
    }
    *out_sum      = sum;
    *out_min      = min_v;
    *out_max      = max_v;
    *out_sum_sq   = sum_sq;
    *out_nz_count = nz;
}

#define CRC32C_POLY 0x82F63B78u

static uint32_t crc32c_table[256];
static int      crc32c_table_ready = 0;

static void build_crc_table(void)
{
    for (uint32_t i = 0; i < 256; i++) {
        uint32_t crc = i;
        for (int j = 0; j < 8; j++)
            crc = (crc & 1u) ? (crc >> 1) ^ CRC32C_POLY : (crc >> 1);
        crc32c_table[i] = crc;
    }
    crc32c_table_ready = 1;
}

uint32_t striq_crc32c(uint32_t crc, const uint8_t *data, size_t len)
{
    if (!crc32c_table_ready) build_crc_table();
    crc = ~crc;
    for (size_t i = 0; i < len; i++)
        crc = (crc >> 8) ^ crc32c_table[(crc ^ data[i]) & 0xFFu];
    return ~crc;
}
