/*
 * test_simd.c — Verify SIMD outputs are identical to scalar reference.
 *
 * The platform/simd.h interface selects NEON or scalar at compile time.
 * We test the SIMD path against its own scalar reference by:
 *   1. Calling striq_byte_shuffle / striq_byte_unshuffle
 *   2. Calling striq_cheb_eval_batch
 *   3. Calling striq_crc32c
 * and comparing to independent scalar implementations below.
 */

#include "../lib/platform/simd.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <math.h>
#include <stdint.h>

static int failures = 0;
#define PASS(name) printf("  PASS: %s\n", name)
#define FAIL(name, ...) do { printf("  FAIL: %s — ", name); printf(__VA_ARGS__); printf("\n"); failures++; } while(0)

static void ref_byte_shuffle(const uint8_t *src, uint8_t *dst,
                              size_t count, size_t elem_size)
{
    for (size_t i = 0; i < count; i++)
        for (size_t b = 0; b < elem_size; b++)
            dst[b * count + i] = src[i * elem_size + b];
}

static void ref_cheb_eval(const double c[4], size_t length, double *out)
{
    if (length == 0) return;
    if (length == 1) { out[0] = c[0]; return; }
    for (size_t i = 0; i < length; i++) {
        double u  = -1.0 + 2.0 * (double)i / (double)(length - 1);
        double b3 = c[3];
        double b2 = c[2] + 2.0 * u * b3;
        double b1 = c[1] + 2.0 * u * b2 - b3;
        out[i]    = c[0] + u * b1 - b2;
    }
}

#define CRC32C_POLY_REF 0x82F63B78u
static uint32_t ref_crc32c(uint32_t crc, const uint8_t *data, size_t len)
{
    static uint32_t table[256];
    static int ready = 0;
    if (!ready) {
        for (uint32_t i = 0; i < 256; i++) {
            uint32_t c = i;
            for (int j = 0; j < 8; j++) c = (c&1u)?(c>>1)^CRC32C_POLY_REF:(c>>1);
            table[i] = c;
        }
        ready = 1;
    }
    crc = ~crc;
    for (size_t i = 0; i < len; i++)
        crc = (crc >> 8) ^ table[(crc ^ data[i]) & 0xFF];
    return ~crc;
}

static void test_shuffle_roundtrip(void)
{
    const int N = 256;
    uint8_t *src = malloc(N * 8);
    uint8_t *shuf = malloc(N * 8);
    uint8_t *back = malloc(N * 8);

    for (int i = 0; i < N * 8; i++) src[i] = (uint8_t)(i * 17 + 3);

    striq_byte_shuffle(src, shuf, N, 8);
    striq_byte_unshuffle(shuf, back, N, 8);

    if (memcmp(src, back, N * 8) != 0)
        FAIL("shuffle_roundtrip", "src != unshuffle(shuffle(src))");
    else
        PASS("shuffle_roundtrip");
    free(src); free(shuf); free(back);
}

static void test_shuffle_matches_reference(void)
{
    const int N = 64;
    uint8_t *src  = malloc(N * 8);
    uint8_t *simd_out = malloc(N * 8);
    uint8_t *ref_out  = malloc(N * 8);

    for (int i = 0; i < N * 8; i++) src[i] = (uint8_t)(i ^ 0xAB);

    striq_byte_shuffle(src, simd_out, N, 8);
    ref_byte_shuffle(src, ref_out, N, 8);

    if (memcmp(simd_out, ref_out, N * 8) != 0)
        FAIL("shuffle_matches_reference", "SIMD output differs from scalar");
    else
        PASS("shuffle_matches_reference");
    free(src); free(simd_out); free(ref_out);
}

static void test_cheb_eval_batch(void)
{
    const double c[4] = { 5.0, 3.0, -1.0, 0.5 };
    const int N = 101;
    double *simd_out = malloc(N * sizeof(double));
    double *ref_out  = malloc(N * sizeof(double));

    striq_cheb_eval_batch(c, N, simd_out);
    ref_cheb_eval(c, N, ref_out);

    for (int i = 0; i < N; i++) {
        if (fabs(simd_out[i] - ref_out[i]) > 1e-10) {
            FAIL("cheb_eval_batch", "mismatch at i=%d: %.15g vs %.15g",
                 i, simd_out[i], ref_out[i]);
            free(simd_out); free(ref_out); return;
        }
    }
    PASS("cheb_eval_batch");
    free(simd_out); free(ref_out);
}

static void test_cheb_eval_single(void)
{
    const double c[4] = { 1.0, 0.0, 0.0, 0.0 };
    double out[1];
    striq_cheb_eval_batch(c, 1, out);
    if (fabs(out[0] - 1.0) > 1e-12)
        FAIL("cheb_eval_single", "c[0]=1 should give 1.0, got %.6f", out[0]);
    else
        PASS("cheb_eval_single");
}

static void test_crc32c_known(void)
{
    /* CRC32C("123456789") = 0xE3069283 */
    const uint8_t data[] = "123456789";
    uint32_t got = striq_crc32c(0, data, 9);
    if (got != 0xE3069283u)
        FAIL("crc32c_known", "expected 0xE3069283, got 0x%08X", got);
    else
        PASS("crc32c_known");
}

static void test_crc32c_matches_reference(void)
{
    const int LEN = 4096;
    uint8_t *data = malloc(LEN);
    for (int i = 0; i < LEN; i++) data[i] = (uint8_t)(i * 13 + 7);

    uint32_t simd_val = striq_crc32c(0, data, LEN);
    uint32_t ref_val  = ref_crc32c(0, data, LEN);

    if (simd_val != ref_val)
        FAIL("crc32c_matches_reference", "SIMD 0x%08X != scalar 0x%08X",
             simd_val, ref_val);
    else
        PASS("crc32c_matches_reference");
    free(data);
}

static void test_crc32c_incremental(void)
{
    const int LEN = 1000;
    uint8_t *data = malloc(LEN);
    for (int i = 0; i < LEN; i++) data[i] = (uint8_t)(i);

    uint32_t full   = striq_crc32c(0, data, LEN);
    uint32_t part1  = striq_crc32c(0,     data,       LEN/2);
    uint32_t part2  = striq_crc32c(part1, data + LEN/2, LEN - LEN/2);

    if (full != part2)
        FAIL("crc32c_incremental", "full=0x%08X != incremental=0x%08X", full, part2);
    else
        PASS("crc32c_incremental");
    free(data);
}

static void test_shuffle_improves_lz4_pattern(void)
{
    /* Doubles in range [1000.001 .. 1000.256] share the same top 5 bytes.
     * After shuffle, byte plane 7 (MSB) should be all-identical. */
    const int N = 256;
    double *vals = malloc(N * sizeof(double));
    for (int i = 0; i < N; i++) vals[i] = 1000.0 + i * 0.001;

    uint8_t *src  = (uint8_t *)vals;
    uint8_t *shuf = malloc(N * 8);
    striq_byte_shuffle(src, shuf, N, 8);

    /* Check byte plane 7 (MSB) is all the same */
    uint8_t first = shuf[7 * N];
    int all_same = 1;
    for (int i = 0; i < N; i++) {
        if (shuf[7 * N + i] != first) { all_same = 0; break; }
    }
    if (all_same)
        PASS("shuffle_improves_lz4_pattern (MSB plane uniform)");
    else
        FAIL("shuffle_improves_lz4_pattern", "MSB plane is not uniform");

    free(vals); free(shuf);
}

int main(void)
{
    printf("=== SIMD platform tests ===\n");
#if defined(STRIQ_HAS_NEON)
    printf("  Platform: NEON (arm64)\n");
#else
    printf("  Platform: scalar fallback\n");
#endif

    test_shuffle_roundtrip();
    test_shuffle_matches_reference();
    test_cheb_eval_batch();
    test_cheb_eval_single();
    test_crc32c_known();
    test_crc32c_matches_reference();
    test_crc32c_incremental();
    test_shuffle_improves_lz4_pattern();

    if (failures == 0)
        printf("=== All SIMD tests PASSED ===\n");
    else
        printf("=== %d SIMD test(s) FAILED ===\n", failures);
    return failures ? 1 : 0;
}
