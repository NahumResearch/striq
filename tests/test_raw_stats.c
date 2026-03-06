#include <stdio.h>
#include <assert.h>
#include <math.h>
#include <stdlib.h>
#include "../lib/core/codecs/raw_stats.h"
#include "../lib/core/routing/router.h"
#include "../lib/core/types.h"

#define EPS 1e-9

static void test_roundtrip(void)
{
    const int N = 512;
    double src[512], dst[512];
    srand(42);
    for (int i = 0; i < N; i++)
        src[i] = -100.0 + (double)rand() / RAND_MAX * 200.0;

    uint8_t buf[RAW_STATS_HDR_SIZE + N * 8 + 16];
    size_t encoded = raw_stats_encode(src, N, buf, sizeof(buf));
    assert(encoded == RAW_STATS_HDR_SIZE + N * 8);

    assert(raw_stats_decode(buf, encoded, dst, N) == STRIQ_OK);
    for (int i = 0; i < N; i++)
        assert(src[i] == dst[i]);

    printf("  [PASS] roundtrip: encode → decode is exact\n");
}

static void test_header_mean(void)
{
    const int N = 1000;
    double vals[1000];
    double expected_sum = 0.0;
    for (int i = 0; i < N; i++) {
        vals[i] = (double)(i + 1);
        expected_sum += vals[i];
    }

    uint8_t buf[RAW_STATS_HDR_SIZE + N * 8 + 16];
    size_t encoded = raw_stats_encode(vals, N, buf, sizeof(buf));
    assert(encoded > 0);

    striq_raw_stats_hdr_t hdr;
    assert(raw_stats_parse_hdr(buf, encoded, &hdr) == STRIQ_OK);

    assert(hdr.count == (uint64_t)N);
    assert(fabs(hdr.sum - expected_sum) < EPS);

    double mean = hdr.sum / (double)hdr.count;
    /* Arithmetic mean of 1..N = (N+1)/2 */
    assert(fabs(mean - 500.5) < EPS);

    printf("  [PASS] header mean: %.1f (expected 500.5)\n", mean);
}

static void test_header_minmax(void)
{
    const int N = 200;
    double vals[200];
    for (int i = 0; i < N; i++)
        vals[i] = -50.0 + (double)i * 0.5;  /* -50.0 .. 49.5 */

    uint8_t buf[RAW_STATS_HDR_SIZE + N * 8 + 16];
    size_t encoded = raw_stats_encode(vals, N, buf, sizeof(buf));
    assert(encoded > 0);

    striq_raw_stats_hdr_t hdr;
    assert(raw_stats_parse_hdr(buf, encoded, &hdr) == STRIQ_OK);

    assert(fabs(hdr.min - (-50.0)) < EPS);
    assert(fabs(hdr.max - 49.5) < EPS);

    printf("  [PASS] header min=%.1f max=%.1f\n", hdr.min, hdr.max);
}

static void test_header_variance(void)
{
    /* Constant signal: var = 0 */
    const int N = 100;
    double vals[100];
    for (int i = 0; i < N; i++) vals[i] = 3.14;

    uint8_t buf[RAW_STATS_HDR_SIZE + N * 8 + 16];
    size_t encoded = raw_stats_encode(vals, N, buf, sizeof(buf));
    assert(encoded > 0);

    striq_raw_stats_hdr_t hdr;
    assert(raw_stats_parse_hdr(buf, encoded, &hdr) == STRIQ_OK);

    double mean = hdr.sum / (double)hdr.count;
    double var  = hdr.sum_sq / (double)hdr.count - mean * mean;
    assert(fabs(var) < EPS);
    assert(fabs(mean - 3.14) < EPS);

    printf("  [PASS] header variance for constant signal = %.2e\n", var);
}

static void test_router_noise_goes_raw_stats(void)
{
    double vals[1000];
    srand(99);
    for (int i = 0; i < 1000; i++) vals[i] = (double)rand() / RAND_MAX;

    striq_codec_t codec;
    assert(router_select(vals, 1000, 0.0, &codec) == STRIQ_OK);
    assert(codec == CODEC_RAW_STATS);

    printf("  [PASS] router sends white_noise → CODEC_RAW_STATS\n");
}

static void test_router_ramp_not_raw_stats(void)
{
    double vals[1000];
    for (int i = 0; i < 1000; i++) vals[i] = (double)i * 1.5 + 10.0;

    striq_codec_t codec;
    assert(router_select(vals, 1000, 0.0, &codec) == STRIQ_OK);
    assert(codec == CODEC_PLA_LINEAR);

    printf("  [PASS] router sends ramp → CODEC_PLA_LINEAR (not RAW_STATS)\n");
}

int main(void)
{
    printf("=== test_raw_stats ===\n");
    test_roundtrip();
    test_header_mean();
    test_header_minmax();
    test_header_variance();
    test_router_noise_goes_raw_stats();
    test_router_ramp_not_raw_stats();
    printf("=== All raw_stats tests PASSED ===\n");
    return 0;
}
