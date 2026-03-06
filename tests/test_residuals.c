#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>
#include <math.h>
#include <stdbool.h>
#include "../lib/core/codecs/residuals.h"
#include "../lib/core/codecs/pla.h"

#define BIG (4 * 1024 * 1024)

static void check_residual_roundtrip(const int64_t *resids, size_t n, const char *label)
{
    size_t cap = residuals_encode_bound(n);
    uint8_t *enc = malloc(cap);
    assert(enc);

    size_t enc_len = 0;
    assert(residuals_encode_auto(resids, n, enc, cap, &enc_len) == STRIQ_OK);

    int64_t *dec = malloc(n * sizeof(int64_t));
    assert(dec);
    assert(residuals_decode(enc, enc_len, dec, n) == STRIQ_OK);

    for (size_t i = 0; i < n; i++) {
        if (dec[i] != resids[i]) {
            fprintf(stderr, "FAIL %s: i=%zu expected=%lld got=%lld\n",
                    label, i, (long long)resids[i], (long long)dec[i]);
            assert(0);
        }
    }

    printf("  [PASS] %s (n=%zu encoded=%zu bytes flag=0x%02x)\n",
           label, n, enc_len, enc[0]);
    free(enc);
    free(dec);
}

static void test_all_zeros(void)
{
    size_t n = 10000;
    int64_t *r = calloc(n, sizeof(int64_t));
    assert(r);

    size_t cap = residuals_encode_bound(n);
    uint8_t *enc = malloc(cap);
    assert(enc);
    size_t enc_len = 0;
    assert(residuals_encode_auto(r, n, enc, cap, &enc_len) == STRIQ_OK);

    /* Zero run of 10000 → flag(1) + 0x00(1) + varint(10000)(3) = 5 bytes */
    assert(enc_len <= 10);
    printf("  [PASS] all_zeros (n=%zu encoded=%zu bytes)\n", n, enc_len);

    int64_t *dec = calloc(n, sizeof(int64_t));
    assert(residuals_decode(enc, enc_len, dec, n) == STRIQ_OK);
    for (size_t i = 0; i < n; i++) assert(dec[i] == 0);
    printf("  [PASS] all_zeros decode verified\n");

    free(r); free(enc); free(dec);
}

static void test_all_nonzero(void)
{
    size_t n = 500;
    int64_t r[500];
    for (size_t i = 0; i < n; i++) r[i] = (int64_t)(i + 1) * 37 - 9000;
    check_residual_roundtrip(r, n, "all_nonzero");
}

static void test_mixed(void)
{
    size_t n = 1024;
    int64_t *r = calloc(n, sizeof(int64_t));
    assert(r);
    /* every 10th residual is nonzero */
    for (size_t i = 0; i < n; i += 10) r[i] = (int64_t)(i * 3 + 1);
    check_residual_roundtrip(r, n, "mixed");
    free(r);
}

static void test_single(void)
{
    int64_t r1 = 0;
    check_residual_roundtrip(&r1, 1, "single_zero");
    int64_t r2 = 42;
    check_residual_roundtrip(&r2, 1, "single_nonzero");
    int64_t r3 = -1000000;
    check_residual_roundtrip(&r3, 1, "single_negative");
}

static void test_pla_ramp_compression(void)
{
    size_t n = 4000;
    double *vals = malloc(n * sizeof(double));
    assert(vals);
    for (size_t i = 0; i < n; i++) vals[i] = (double)i * 0.5;

    uint8_t *seg_buf   = malloc(BIG);
    uint8_t *resid_buf = malloc(BIG);
    assert(seg_buf && resid_buf);

    size_t seg_count = 0, resid_len = 0;
    bool used_cheb = false;
    assert(pla_encode(vals, n, 0.001, 0, seg_buf, BIG, &seg_count, &used_cheb, resid_buf, BIG, &resid_len) == STRIQ_OK);

    /* Ramp: residuals should be near zero → RLE compresses to tiny size */
    double raw_bytes = (double)(n * sizeof(double));
    double compressed = (double)(seg_count * 18 + resid_len);
    double ratio = raw_bytes / compressed;

    printf("  [PASS] pla_ramp_compression: segs=%zu resid=%zu bytes ratio=%.1fx\n",
           seg_count, resid_len, ratio);

    /* Decode and verify */
    double *out = malloc(n * sizeof(double));
    assert(out);
    assert(pla_decode(seg_buf, seg_count, false, resid_buf, resid_len, n, out) == STRIQ_OK);
    for (size_t i = 0; i < n; i++) {
        double diff = fabs(out[i] - vals[i]);
        if (diff > 0.002) {
            fprintf(stderr, "  FAIL pla_ramp i=%zu diff=%.6f\n", i, diff);
            assert(0);
        }
    }

    free(vals); free(seg_buf); free(resid_buf); free(out);
}

static void test_pla_sine_compression(void)
{
    size_t n = 4000;
    double *vals = malloc(n * sizeof(double));
    assert(vals);
    for (size_t i = 0; i < n; i++) vals[i] = sin((double)i * 0.05) * 100.0;

    uint8_t *seg_buf   = malloc(BIG);
    uint8_t *resid_buf = malloc(BIG);
    assert(seg_buf && resid_buf);

    size_t seg_count = 0, resid_len = 0;
    bool   used_cheb = false;
    double eps = 0.1;
    assert(pla_encode(vals, n, eps, 0, seg_buf, BIG, &seg_count, &used_cheb, resid_buf, BIG, &resid_len) == STRIQ_OK);

    double *out = malloc(n * sizeof(double));
    assert(out);
    assert(pla_decode(seg_buf, seg_count, false, resid_buf, resid_len, n, out) == STRIQ_OK);
    for (size_t i = 0; i < n; i++) {
        double diff = fabs(out[i] - vals[i]);
        if (diff > eps * 2.0 + 1e-6) {
            fprintf(stderr, "  FAIL pla_sine i=%zu diff=%.6f eps=%.6f\n", i, diff, eps);
            assert(0);
        }
    }

    printf("  [PASS] pla_sine_compression: segs=%zu resid=%zu bytes\n",
           seg_count, resid_len);

    free(vals); free(seg_buf); free(resid_buf); free(out);
}

static void test_empty(void)
{
    uint8_t buf[16];
    size_t len = 0;
    assert(residuals_encode_auto(NULL, 0, buf, sizeof(buf), &len) == STRIQ_OK);
    assert(len == 0);

    int64_t out[1];
    assert(residuals_decode(buf, 0, out, 0) == STRIQ_OK);
    printf("  [PASS] empty input\n");
}

static void test_flags(void)
{
    /* All-zero residuals should produce RLE flag (tiny output, no LZ4 benefit) */
    int64_t zeros[100] = {0};
    size_t cap = residuals_encode_bound(100);
    uint8_t *enc = malloc(cap);
    assert(enc);
    size_t len = 0;
    assert(residuals_encode_auto(zeros, 100, enc, cap, &len) == STRIQ_OK);
    assert(enc[0] == RESID_FLAG_RLE || enc[0] == RESID_FLAG_RLE_I8 ||
           enc[0] == RESID_FLAG_RLE_I16);
    printf("  [PASS] flags (flag=0x%02x len=%zu)\n", enc[0], len);
    free(enc);
}

int main(void)
{
    printf("=== test_residuals ===\n");
    test_empty();
    test_all_zeros();
    test_all_nonzero();
    test_mixed();
    test_single();
    test_flags();
    test_pla_ramp_compression();
    test_pla_sine_compression();
    printf("All residual tests passed.\n");
    return 0;
}
