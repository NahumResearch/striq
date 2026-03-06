#include "../lib/core/codecs/decimal.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <math.h>
#include <assert.h>

#ifndef M_PI
#define M_PI 3.14159265358979323846
#endif
#ifndef M_E
#define M_E 2.71828182845904523536
#endif
#ifndef M_SQRT2
#define M_SQRT2 1.41421356237309504880
#endif

static int failures = 0;

#define CHECK(cond, msg) do { \
    if (!(cond)) { fprintf(stderr, "FAIL: %s\n", msg); failures++; } \
    else         { printf("  PASS: %s\n", msg); } \
} while(0)

static void test_detect_temperature(void)
{
    printf("test_detect_temperature\n");
    double temps[] = {20.1, 20.3, 20.5, 20.4, 20.2, 19.9, 20.0, 20.1};
    uint32_t n = sizeof(temps) / sizeof(temps[0]);
    uint8_t d, db;
    bool ok = decimal_detect(temps, n, &d, &db);
    CHECK(ok, "detects decimal origin");
    CHECK(d == 1, "scale = 10^1");
    CHECK(db == 1, "delta_bytes = 1 (small deltas)");
}

static void test_detect_reject_irrational(void)
{
    printf("test_detect_reject_irrational\n");
    double irr[] = {M_PI, M_E, M_SQRT2, 1.41421356237310, 2.71828182845905};
    uint32_t n = sizeof(irr) / sizeof(irr[0]);
    uint8_t d, db;
    bool ok = decimal_detect(irr, n, &d, &db);
    CHECK(!ok, "irrational data rejected");
}

static void test_roundtrip_temperature(void)
{
    printf("test_roundtrip_temperature\n");
    double src[128];
    for (int i = 0; i < 128; i++)
        src[i] = 18.0 + (double)(i % 40) * 0.1;

    uint8_t buf[4096];
    size_t enc_len = decimal_encode(src, 128, buf, sizeof(buf));
    CHECK(enc_len > 0, "encode succeeds");

    /* Should be much smaller than raw (128×8 = 1024 bytes) */
    CHECK(enc_len < 300, "compressed size < 300 bytes (vs 1024 raw)");

    double dst[128];
    striq_status_t s = decimal_decode(buf, enc_len, dst, 128);
    CHECK(s == STRIQ_OK, "decode succeeds");

    bool exact = true;
    for (int i = 0; i < 128; i++) {
        if (fabs(dst[i] - src[i]) > 1e-9) { exact = false; break; }
    }
    CHECK(exact, "round-trip lossless");
}

static void test_roundtrip_voltage(void)
{
    printf("test_roundtrip_voltage\n");
    double src[256];
    for (int i = 0; i < 256; i++)
        src[i] = 230.0 + (double)(i % 20) * 0.2 - 2.0;

    uint8_t buf[8192];
    size_t enc_len = decimal_encode(src, 256, buf, sizeof(buf));
    CHECK(enc_len > 0, "voltage encode succeeds");

    double dst[256];
    striq_status_t s = decimal_decode(buf, enc_len, dst, 256);
    CHECK(s == STRIQ_OK, "decode succeeds");

    bool exact = true;
    for (int i = 0; i < 256; i++) {
        if (fabs(dst[i] - src[i]) > 1e-9) { exact = false; break; }
    }
    CHECK(exact, "voltage round-trip lossless");
}

static void test_integer_data(void)
{
    printf("test_integer_data\n");
    double src[] = {100.0, 101.0, 102.0, 103.0, 104.0, 103.0, 102.0, 101.0};
    uint32_t n = 8;
    uint8_t d, db;
    bool ok = decimal_detect(src, n, &d, &db);
    CHECK(ok, "integer data detected");
    CHECK(d == 0, "scale = 10^0");

    uint8_t buf[1024];
    size_t enc_len = decimal_encode(src, n, buf, sizeof(buf));
    CHECK(enc_len > 0, "encode");
    double dst[8];
    CHECK(decimal_decode(buf, enc_len, dst, 8) == STRIQ_OK, "decode");
    bool match = true;
    for (uint32_t i = 0; i < n; i++)
        if (fabs(dst[i] - src[i]) > 1e-9) { match = false; break; }
    CHECK(match, "integer round-trip exact");
}

static void test_compression_ratio(void)
{
    printf("test_compression_ratio\n");
    const uint32_t N = 4096;
    double *src = malloc(N * sizeof(double));
    assert(src);
    for (uint32_t i = 0; i < N; i++)
        src[i] = 20.0 + (double)(i % 100) * 0.1;

    uint8_t *buf = malloc(DECIMAL_TOTAL_HDR_BYTES + N * 4 + 16);
    assert(buf);
    size_t enc_len = decimal_encode(src, N, buf, DECIMAL_TOTAL_HDR_BYTES + N * 4 + 16);
    CHECK(enc_len > 0, "encode 4096-point block");

    size_t raw_size = 48 + N * 8;
    double ratio = (double)raw_size / (double)enc_len;
    printf("  compression ratio: %.2fx  (enc=%zu raw=%zu)\n", ratio, enc_len, raw_size);
    CHECK(ratio >= 4.0, "ratio >= 4x vs raw");

    double *dst = malloc(N * sizeof(double));
    assert(dst);
    CHECK(decimal_decode(buf, enc_len, dst, N) == STRIQ_OK, "decode 4096-point block");

    bool exact = true;
    for (uint32_t i = 0; i < N; i++) {
        if (fabs(dst[i] - src[i]) > 1e-9) { exact = false; break; }
    }
    CHECK(exact, "4096-point round-trip lossless");

    free(src); free(buf); free(dst);
}

int main(void)
{
    printf("=== test_decimal ===\n");
    test_detect_temperature();
    test_detect_reject_irrational();
    test_roundtrip_temperature();
    test_roundtrip_voltage();
    test_integer_data();
    test_compression_ratio();

    if (failures == 0)
        printf("\nAll decimal tests PASSED\n");
    else
        printf("\n%d decimal test(s) FAILED\n", failures);
    return failures ? 1 : 0;
}
