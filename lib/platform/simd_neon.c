
#include "simd.h"

#if defined(STRIQ_HAS_NEON)

#include <string.h>
#include <math.h>
#include <arm_neon.h>


#if defined(__ARM_FEATURE_CRC32)
  #include <arm_acle.h>
#else
  #undef STRIQ_HAS_NEON_CRC
#endif

#ifndef STRIQ_HAS_NEON_CRC
  #if defined(__ARM_FEATURE_CRC32)
    #define STRIQ_HAS_NEON_CRC 1
  #endif
#endif

void striq_byte_shuffle(const uint8_t *src, uint8_t *dst,
                        size_t count, size_t elem_size)
{

    if (elem_size == 8) {
        size_t batch = count & ~(size_t)7;
        for (size_t i = 0; i < batch; i += 8) {
            uint8x16_t r0 = vld1q_u8(src + i*8 +  0);
            uint8x16_t r1 = vld1q_u8(src + i*8 + 16);
            uint8x16_t r2 = vld1q_u8(src + i*8 + 32);
            uint8x16_t r3 = vld1q_u8(src + i*8 + 48);
            uint8x16x2_t t01 = vuzpq_u8(r0, r1);
            uint8x16x2_t t23 = vuzpq_u8(r2, r3);
            uint8x16x2_t u0 = vuzpq_u8(t01.val[0], t23.val[0]);
            uint8x16x2_t u1 = vuzpq_u8(t01.val[1], t23.val[1]);
            uint8x16x2_t v0 = vuzpq_u8(u0.val[0], u1.val[0]);
            uint8x16x2_t v1 = vuzpq_u8(u0.val[1], u1.val[1]);
            (void)v0; (void)v1;
            goto scalar_path;
        }
        return;
    }

scalar_path:
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

    float64x2_t vc0 = vdupq_n_f64(c[0]);
    float64x2_t vc1 = vdupq_n_f64(c[1]);
    float64x2_t vc2 = vdupq_n_f64(c[2]);
    float64x2_t vc3 = vdupq_n_f64(c[3]);
    float64x2_t two = vdupq_n_f64(2.0);

    double inv_len = 2.0 / (double)(length - 1);
    size_t i = 0;

    for (; i + 1 < length; i += 2) {
        double u0 = -1.0 + (double)i       * inv_len;
        double u1 = -1.0 + (double)(i + 1) * inv_len;
        float64x2_t u = {u0, u1};

        /* Clenshaw: b3 = c3 */
        float64x2_t b3 = vc3;
        /* b2 = c2 + 2*u*b3 */
        float64x2_t b2 = vfmaq_f64(vc2, vmulq_f64(two, u), b3);
        /* b1 = c1 + 2*u*b2 - b3 */
        float64x2_t b1 = vsubq_f64(vfmaq_f64(vc1, vmulq_f64(two, u), b2), b3);
        /* result = c0 + u*b1 - b2 */
        float64x2_t res = vsubq_f64(vfmaq_f64(vc0, u, b1), b2);

        vst1q_f64(out + i, res);
    }

    /* Scalar remainder (at most 1 point) */
    for (; i < length; i++) {
        double u  = -1.0 + (double)i * inv_len;
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

    float64x2_t vsum   = vdupq_n_f64(0.0);
    float64x2_t vsumsq = vdupq_n_f64(0.0);
    float64x2_t vmin   = vdupq_n_f64(data[0]);
    float64x2_t vmax   = vdupq_n_f64(data[0]);
    uint64_t nz = 0;

    size_t i = 0;
    for (; i + 1 < n; i += 2) {
        float64x2_t v = vld1q_f64(data + i);
        vsum   = vaddq_f64(vsum, v);
        vsumsq = vfmaq_f64(vsumsq, v, v);
        vmin   = vminnmq_f64(vmin, v);
        vmax   = vmaxnmq_f64(vmax, v);
        if (data[i]   != 0.0) nz++;
        if (data[i+1] != 0.0) nz++;
    }

    double sum    = vgetq_lane_f64(vsum,   0) + vgetq_lane_f64(vsum,   1);
    double sum_sq = vgetq_lane_f64(vsumsq, 0) + vgetq_lane_f64(vsumsq, 1);
    double min_v  = fmin(vgetq_lane_f64(vmin, 0), vgetq_lane_f64(vmin, 1));
    double max_v  = fmax(vgetq_lane_f64(vmax, 0), vgetq_lane_f64(vmax, 1));

    for (; i < n; i++) {
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

#if defined(STRIQ_HAS_NEON_CRC)

uint32_t striq_crc32c(uint32_t crc, const uint8_t *data, size_t len)
{
    crc = ~crc;
    size_t i = 0;
    for (; i + 8 <= len; i += 8) {
        uint64_t word;
        __builtin_memcpy(&word, data + i, 8);
        crc = (uint32_t)__crc32cd(crc, word);
    }
    if (i + 4 <= len) {
        uint32_t word;
        __builtin_memcpy(&word, data + i, 4);
        crc = __crc32cw(crc, word);
        i += 4;
    }
    if (i + 2 <= len) {
        uint16_t word;
        __builtin_memcpy(&word, data + i, 2);
        crc = __crc32ch(crc, word);
        i += 2;
    }
    if (i < len)
        crc = __crc32cb(crc, data[i]);
    return ~crc;
}

#else /* no hardware CRC — use software table */

#define CRC32C_POLY 0x82F63B78u
static uint32_t s_crc_table[256];
static int s_crc_ready = 0;
static void build_s_table(void) {
    for (uint32_t i = 0; i < 256; i++) {
        uint32_t c = i;
        for (int j = 0; j < 8; j++) c = (c&1u)?(c>>1)^CRC32C_POLY:(c>>1);
        s_crc_table[i] = c;
    }
    s_crc_ready = 1;
}
uint32_t striq_crc32c(uint32_t crc, const uint8_t *data, size_t len) {
    if (!s_crc_ready) build_s_table();
    crc = ~crc;
    for (size_t i = 0; i < len; i++)
        crc = (crc >> 8) ^ s_crc_table[(crc ^ data[i]) & 0xFF];
    return ~crc;
}
#endif

#else /* !STRIQ_HAS_NEON — should not be compiled on non-NEON but guard anyway */

#include "simd_scalar.c"

#endif /* STRIQ_HAS_NEON */
