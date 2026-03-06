#include "../lib/core/codecs/quant.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <math.h>
#include <assert.h>

static int failures = 0;

#define CHECK(cond, msg) do { \
    if (!(cond)) { fprintf(stderr, "FAIL: %s\n", msg); failures++; } \
    else         { printf("  PASS: %s\n", msg); } \
} while(0)

static void test_quant16_roundtrip(void)
{
    printf("test_quant16_roundtrip\n");
    double src[256];
    double range = 100.0;
    for (int i = 0; i < 256; i++)
        src[i] = 50.0 + range * sin((double)i * 0.1);

    uint8_t buf[8192];
    size_t enc = quant_encode(src, 256, 16, buf, sizeof(buf));
    CHECK(enc > 0, "encode succeeds");

    double expected_max_err = range * 2.0 / 65535.0;
    double dst[256];
    CHECK(quant_decode(buf, enc, 16, dst, 256) == STRIQ_OK, "decode succeeds");

    bool within_eps = true;
    for (int i = 0; i < 256; i++) {
        if (fabs(dst[i] - src[i]) > expected_max_err + 1e-9)
            { within_eps = false; break; }
    }
    CHECK(within_eps, "all values within theoretical max error");
}

static void test_quant8_roundtrip(void)
{
    printf("test_quant8_roundtrip\n");
    double src[128];
    double range = 50.0;
    for (int i = 0; i < 128; i++)
        src[i] = range * (double)i / 127.0;

    uint8_t buf[4096];
    size_t enc = quant_encode(src, 128, 8, buf, sizeof(buf));
    CHECK(enc > 0, "encode 8-bit");

    double expected_eps = range / 255.0;
    double dst[128];
    CHECK(quant_decode(buf, enc, 8, dst, 128) == STRIQ_OK, "decode 8-bit");

    bool within_eps = true;
    for (int i = 0; i < 128; i++) {
        if (fabs(dst[i] - src[i]) > expected_eps + 1e-9)
            { within_eps = false; break; }
    }
    CHECK(within_eps, "QUANT8 within epsilon");
}

static void test_compression_ratios(void)
{
    printf("test_compression_ratios\n");
    const uint32_t N = 4096;
    double *src = malloc(N * sizeof(double));
    assert(src);
    for (uint32_t i = 0; i < N; i++)
        src[i] = (double)(i % 200) * 0.5;

    uint8_t *buf = malloc(QUANT_TOTAL_HDR_BYTES + N * 2 + 16);
    assert(buf);

    size_t enc16 = quant_encode(src, N, 16, buf, QUANT_TOTAL_HDR_BYTES + N * 2 + 16);
    size_t raw_size = 48 + N * 8;
    double ratio16 = (double)raw_size / (double)enc16;
    printf("  QUANT16 ratio: %.2fx  (enc=%zu raw=%zu)\n", ratio16, enc16, raw_size);
    CHECK(ratio16 >= 3.5, "QUANT16 ratio >= 3.5x");

    free(buf);
    buf = malloc(QUANT_TOTAL_HDR_BYTES + N + 16);
    assert(buf);
    size_t enc8 = quant_encode(src, N, 8, buf, QUANT_TOTAL_HDR_BYTES + N + 16);
    double ratio8 = (double)raw_size / (double)enc8;
    printf("  QUANT8  ratio: %.2fx  (enc=%zu raw=%zu)\n", ratio8, enc8, raw_size);
    CHECK(ratio8 >= 7.0, "QUANT8 ratio >= 7x");

    free(src); free(buf);
}

static void test_constant_signal(void)
{
    printf("test_constant_signal\n");
    double src[64];
    for (int i = 0; i < 64; i++) src[i] = 42.0;

    uint8_t buf[2048];
    size_t enc = quant_encode(src, 64, 16, buf, sizeof(buf));
    CHECK(enc > 0, "encode constant");

    double dst[64];
    CHECK(quant_decode(buf, enc, 16, dst, 64) == STRIQ_OK, "decode constant");
    bool ok = true;
    for (int i = 0; i < 64; i++)
        if (fabs(dst[i] - 42.0) > 1e-9) { ok = false; break; }
    CHECK(ok, "constant signal preserved");
}

static void test_stats_header_compatible(void)
{
    printf("test_stats_header_compatible\n");
    double src[] = {1.0, 2.0, 3.0, 4.0, 5.0};
    uint8_t buf[2048];
    size_t enc = quant_encode(src, 5, 16, buf, sizeof(buf));
    CHECK(enc > 0, "encode");

    striq_raw_stats_hdr_t hdr;
    CHECK(raw_stats_parse_hdr(buf, enc, &hdr) == STRIQ_OK, "stats header parseable");
    CHECK(fabs(hdr.min - 1.0) < 1e-9, "min correct");
    CHECK(fabs(hdr.max - 5.0) < 1e-9, "max correct");
    CHECK(fabs(hdr.sum - 15.0) < 1e-9, "sum correct");
    CHECK(hdr.count == 5, "count correct");
}

int main(void)
{
    printf("=== test_quant ===\n");
    test_quant16_roundtrip();
    test_quant8_roundtrip();
    test_compression_ratios();
    test_constant_signal();
    test_stats_header_compatible();

    if (failures == 0)
        printf("\nAll quant tests PASSED\n");
    else
        printf("\n%d quant test(s) FAILED\n", failures);
    return failures ? 1 : 0;
}
