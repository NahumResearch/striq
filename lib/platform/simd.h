#ifndef STRIQ_SIMD_H
#define STRIQ_SIMD_H

#include <stdint.h>
#include <stddef.h>

#if defined(__aarch64__) || defined(__ARM_NEON)
  #define STRIQ_HAS_NEON 1
  #include <arm_neon.h>
#endif

#if defined(__AVX2__)
  #include <immintrin.h>
#endif

void striq_byte_shuffle(const uint8_t *src, uint8_t *dst,
                        size_t count, size_t elem_size);

void striq_byte_unshuffle(const uint8_t *src, uint8_t *dst,
                          size_t count, size_t elem_size);

void striq_cheb_eval_batch(const double c[4], size_t length, double *out);

void striq_stats_reduce(
    const double *data,
    size_t        n,
    double       *out_sum,
    double       *out_min,
    double       *out_max,
    double       *out_sum_sq,
    uint64_t     *out_nz_count
);

uint32_t striq_crc32c(uint32_t crc, const uint8_t *data, size_t len);

#endif /* STRIQ_SIMD_H */
