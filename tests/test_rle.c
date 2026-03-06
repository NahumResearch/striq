/*
 * test_rle.c — Unit tests for the Value-RLE codec and router integration.
 */
#include "../lib/core/codecs/rle_codec.h"
#include "../lib/core/routing/router.h"
#include "../lib/core/routing/stats.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <math.h>

static int failures = 0;
#define PASS(name) printf("  PASS: %s\n", name)
#define FAIL(name, ...) do { printf("  FAIL: %s — ", name); printf(__VA_ARGS__); printf("\n"); failures++; } while(0)

static void test_step_roundtrip(void)
{
    const int N = 1000;
    double *in = malloc(N * sizeof(double));
    for (int i = 0; i < N; i++) in[i] = (i < 500) ? 0.0 : 1.0;

    uint8_t buf[4096];
    size_t rle_len = 0;
    striq_status_t s = rle_encode(in, N, buf, sizeof buf, &rle_len);
    if (s != STRIQ_OK) { FAIL("step_roundtrip", "encode failed: %d", s); free(in); return; }

    double *out = malloc(N * sizeof(double));
    s = rle_decode(buf, rle_len, out, N);
    if (s != STRIQ_OK) { FAIL("step_roundtrip", "decode failed: %d", s); free(in); free(out); return; }

    for (int i = 0; i < N; i++) {
        if (out[i] != in[i]) {
            FAIL("step_roundtrip", "mismatch at %d: got %.1f want %.1f", i, out[i], in[i]);
            free(in); free(out); return;
        }
    }

    /* Check compression: 2 unique values, 2 runs → tiny output */
    if (rle_len > 100) {
        FAIL("step_roundtrip", "poor compression: %zu bytes for 1000 doubles", rle_len);
    } else {
        printf("  PASS: step_roundtrip (%zu bytes for %d doubles, %.0fx)\n",
               rle_len, N, (double)(N * 8) / (double)rle_len);
    }
    free(in); free(out);
}

static void test_16unique_roundtrip(void)
{
    const int N = 4096;
    double *in = malloc(N * sizeof(double));
    for (int i = 0; i < N; i++) in[i] = (double)(i % 16) * 10.0;

    uint8_t *buf = malloc(N * 20);
    size_t rle_len = 0;
    striq_status_t s = rle_encode(in, N, buf, N * 20, &rle_len);
    if (s != STRIQ_OK) { FAIL("16unique_roundtrip", "encode failed: %d", s); free(in); free(buf); return; }

    double *out = malloc(N * sizeof(double));
    s = rle_decode(buf, rle_len, out, N);
    if (s != STRIQ_OK) { FAIL("16unique_roundtrip", "decode failed"); free(in); free(out); free(buf); return; }

    for (int i = 0; i < N; i++) {
        if (out[i] != in[i]) { FAIL("16unique_roundtrip", "mismatch at %d", i); free(in); free(out); free(buf); return; }
    }
    PASS("16unique_roundtrip");
    free(in); free(out); free(buf);
}

/* Phase 6: 17 unique values ≤ 64 → encode succeeds */
static void test_17unique_succeeds(void)
{
    const int N = 17;
    double in[17];
    for (int i = 0; i < N; i++) in[i] = (double)i;

    uint8_t buf[2048];
    size_t rle_len = 0;
    striq_status_t s = rle_encode(in, N, buf, sizeof buf, &rle_len);
    if (s != STRIQ_OK)
        FAIL("17unique_succeeds", "expected STRIQ_OK, got %d", s);
    else
        PASS("17unique_succeeds");
}

/* 65 unique values > RLE_MAX_UNIQUE (64) → encode fails */
static void test_65unique_fails(void)
{
    const int N = 65;
    double in[65];
    for (int i = 0; i < N; i++) in[i] = (double)i;

    uint8_t buf[4096];
    size_t rle_len = 0;
    striq_status_t s = rle_encode(in, N, buf, sizeof buf, &rle_len);
    if (s == STRIQ_ERR_CODEC)
        PASS("65unique_fails");
    else
        FAIL("65unique_fails", "expected STRIQ_ERR_CODEC, got %d", s);
}

/* Phase 6 plan: 30 unique values → router selects CODEC_RLE */
static void test_router_30unique_goes_rle(void)
{
    const int N = 3000;
    double *vals = malloc(N * sizeof(double));
    for (int i = 0; i < N; i++) vals[i] = (double)(i % 30);

    striq_codec_t codec;
    striq_status_t s = router_select(vals, N, 0.0, &codec);
    if (s != STRIQ_OK || codec != CODEC_RLE)
        FAIL("router_30unique_goes_rle", "expected CODEC_RLE, got codec=%d", (int)codec);
    else
        PASS("router_30unique_goes_rle");
    free(vals);
}

/* Phase 6 plan: 65 unique values → router does NOT select CODEC_RLE */
static void test_router_65unique_not_rle(void)
{
    const int N = 6500;
    double *vals = malloc(N * sizeof(double));
    for (int i = 0; i < N; i++) vals[i] = (double)(i % 65);

    striq_codec_t codec;
    striq_status_t s = router_select(vals, N, 0.0, &codec);
    if (s != STRIQ_OK || codec == CODEC_RLE)
        FAIL("router_65unique_not_rle", "expected != CODEC_RLE, got codec=%d", (int)codec);
    else
        PASS("router_65unique_not_rle");
    free(vals);
}

static void test_algebraic_mean(void)
{
    const int N = 100;
    double in[100];
    for (int i = 0; i < N; i++) in[i] = (i % 4 == 0) ? 10.0 : 20.0;

    double expected_mean = 0.0;
    for (int i = 0; i < N; i++) expected_mean += in[i];
    expected_mean /= N;

    uint8_t buf[1024];
    size_t rle_len = 0;
    rle_encode(in, N, buf, sizeof buf, &rle_len);

    rle_stats_t st;
    striq_status_t s = rle_query_stats(buf, rle_len, &st);
    if (s != STRIQ_OK) { FAIL("algebraic_mean", "rle_query_stats failed"); return; }

    double err = fabs(st.mean - expected_mean);
    if (err > 1e-10) {
        FAIL("algebraic_mean", "mean %.6f expected %.6f", st.mean, expected_mean);
    } else if (st.count != (uint64_t)N) {
        FAIL("algebraic_mean", "count %llu expected %d", (unsigned long long)st.count, N);
    } else {
        PASS("algebraic_mean");
    }
}

static void test_algebraic_minmax(void)
{
    const int N = 200;
    double in[200];
    for (int i = 0; i < N; i++) {
        if (i % 5 == 0) in[i] = -99.0;
        else if (i % 7 == 0) in[i] = 999.0;
        else in[i] = (double)(i % 3) * 5.0;
    }

    uint8_t buf[4096];
    size_t rle_len = 0;
    striq_status_t s = rle_encode(in, N, buf, sizeof buf, &rle_len);
    if (s != STRIQ_OK) { FAIL("algebraic_minmax", "encode failed"); return; }

    rle_stats_t st;
    s = rle_query_stats(buf, rle_len, &st);
    if (s != STRIQ_OK) { FAIL("algebraic_minmax", "query_stats failed"); return; }

    if (fabs(st.min - (-99.0)) > 1e-10) {
        FAIL("algebraic_minmax", "min %.2f expected -99.0", st.min);
    } else if (fabs(st.max - 999.0) > 1e-10) {
        FAIL("algebraic_minmax", "max %.2f expected 999.0", st.max);
    } else {
        PASS("algebraic_minmax");
    }
}

static void test_router_step_to_rle(void)
{
    const int N = 1000;
    double vals[1000];
    /* Step function: 500 rows at 0.0, 500 rows at 1.0 */
    for (int i = 0; i < N; i++) vals[i] = (i < 500) ? 0.0 : 1.0;

    striq_codec_t codec;
    striq_status_t s = router_select(vals, N, 0.0, &codec);
    if (s != STRIQ_OK) { FAIL("router_step_to_rle", "router failed: %d", s); return; }
    if (codec != CODEC_RLE) {
        FAIL("router_step_to_rle", "expected CODEC_RLE, got %d", (int)codec);
    } else {
        PASS("router_step_to_rle");
    }
}

static void test_router_ramp_not_rle(void)
{
    const int N = 1000;
    double vals[1000];
    for (int i = 0; i < N; i++) vals[i] = i * 0.001;

    striq_codec_t codec;
    striq_status_t s = router_select(vals, N, 0.0, &codec);
    if (s != STRIQ_OK) { FAIL("router_ramp_not_rle", "router failed: %d", s); return; }
    if (codec == CODEC_RLE) {
        FAIL("router_ramp_not_rle", "got CODEC_RLE for a ramp (should be PLA)");
    } else {
        PASS("router_ramp_not_rle");
    }
}

static void test_count_where(void)
{
    const int N = 100;
    double in[100];
    for (int i = 0; i < N; i++) in[i] = (i % 2 == 0) ? 0.0 : 1.0;

    uint8_t buf[1024];
    size_t rle_len = 0;
    rle_encode(in, N, buf, sizeof buf, &rle_len);

    uint64_t cnt = 0;
    striq_status_t s = rle_query_count_where(buf, rle_len, 0.5, STRIQ_CMP_GT, &cnt);
    if (s != STRIQ_OK) { FAIL("count_where", "query failed"); return; }
    if (cnt != 50) {
        FAIL("count_where", "count %llu expected 50", (unsigned long long)cnt);
    } else {
        PASS("count_where");
    }
}

static void test_constant_roundtrip(void)
{
    const int N = 512;
    double in[512];
    for (int i = 0; i < N; i++) in[i] = 42.5;

    uint8_t buf[256];
    size_t rle_len = 0;
    striq_status_t s = rle_encode(in, N, buf, sizeof buf, &rle_len);
    if (s != STRIQ_OK) { FAIL("constant_roundtrip", "encode failed"); return; }

    double out[512];
    s = rle_decode(buf, rle_len, out, N);
    if (s != STRIQ_OK) { FAIL("constant_roundtrip", "decode failed"); return; }
    for (int i = 0; i < N; i++) {
        if (out[i] != 42.5) { FAIL("constant_roundtrip", "mismatch at %d", i); return; }
    }
    printf("  PASS: constant_roundtrip (%zu bytes for %d doubles)\n", rle_len, N);
}

static void test_unique_count(void)
{
    /* Old behavior: max_unique=16 still works */
    double v16[160];
    for (int i = 0; i < 160; i++) v16[i] = (double)(i % 16);
    uint32_t n = stats_unique_count(v16, 160, 16);
    if (n != 16) { FAIL("unique_count_16", "got %u expected 16", n); }
    else PASS("unique_count_16");

    /* 17 unique with max_unique=16 → returns 17 (overflow) */
    double v17[170];
    for (int i = 0; i < 170; i++) v17[i] = (double)(i % 17);
    n = stats_unique_count(v17, 170, 16);
    if (n != 17) { FAIL("unique_count_17_overflow", "got %u expected 17", n); }
    else PASS("unique_count_17_overflow");

    /* Phase 6: max_unique=64 — 30 unique values return 30 */
    double v30[300];
    for (int i = 0; i < 300; i++) v30[i] = (double)(i % 30);
    n = stats_unique_count(v30, 300, 64);
    if (n != 30) { FAIL("unique_count_30_with_64cap", "got %u expected 30", n); }
    else PASS("unique_count_30_with_64cap");

    /* Phase 6: 65 unique with max_unique=64 → returns 65 (overflow) */
    double v65[650];
    for (int i = 0; i < 650; i++) v65[i] = (double)(i % 65);
    n = stats_unique_count(v65, 650, 64);
    if (n != 65) { FAIL("unique_count_65_overflow", "got %u expected 65", n); }
    else PASS("unique_count_65_overflow");
}

int main(void)
{
    printf("=== RLE codec tests ===\n");
    test_step_roundtrip();
    test_16unique_roundtrip();
    test_17unique_succeeds();
    test_65unique_fails();
    test_router_30unique_goes_rle();
    test_router_65unique_not_rle();
    test_algebraic_mean();
    test_algebraic_minmax();
    test_router_step_to_rle();
    test_router_ramp_not_rle();
    test_count_where();
    test_constant_roundtrip();
    test_unique_count();

    if (failures == 0)
        printf("=== All RLE tests PASSED ===\n");
    else
        printf("=== %d RLE test(s) FAILED ===\n", failures);
    return failures ? 1 : 0;
}
