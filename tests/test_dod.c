#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>
#include <stdint.h>
#include "../lib/core/codecs/dod.h"

#define BUF_CAP (1024 * 1024)

static void test_regular(void)
{
    int64_t ts[] = {0, 1000, 2000, 3000, 4000, 5000};
    size_t n = 6;
    uint8_t buf[BUF_CAP]; size_t enc_len = 0;
    assert(dod_encode(ts, n, buf, sizeof(buf), &enc_len) == STRIQ_OK);
    /* Regular interval → dod = 0 → very compact */
    assert(enc_len < 20);

    int64_t out[6];
    assert(dod_decode(buf, enc_len, n, out) == STRIQ_OK);
    for (size_t i = 0; i < n; i++) assert(out[i] == ts[i]);
    printf("  [PASS] test_regular\n");
}

static void test_irregular(void)
{
    int64_t ts[] = {0, 100, 350, 1000, 1001, 9999};
    size_t n = 6;
    uint8_t buf[BUF_CAP]; size_t enc_len = 0;
    assert(dod_encode(ts, n, buf, sizeof(buf), &enc_len) == STRIQ_OK);

    int64_t out[6];
    assert(dod_decode(buf, enc_len, n, out) == STRIQ_OK);
    for (size_t i = 0; i < n; i++) assert(out[i] == ts[i]);
    printf("  [PASS] test_irregular\n");
}

static void test_negative(void)
{
    int64_t ts[] = {-5000, -4000, -3000, -2000};
    size_t n = 4;
    uint8_t buf[BUF_CAP]; size_t enc_len = 0;
    assert(dod_encode(ts, n, buf, sizeof(buf), &enc_len) == STRIQ_OK);

    int64_t out[4];
    assert(dod_decode(buf, enc_len, n, out) == STRIQ_OK);
    for (size_t i = 0; i < n; i++) assert(out[i] == ts[i]);
    printf("  [PASS] test_negative\n");
}

static void test_single(void)
{
    int64_t ts[] = {42};
    uint8_t buf[BUF_CAP]; size_t enc_len = 0;
    assert(dod_encode(ts, 1, buf, sizeof(buf), &enc_len) == STRIQ_OK);
    assert(enc_len == 8);

    int64_t out[1];
    assert(dod_decode(buf, enc_len, 1, out) == STRIQ_OK);
    assert(out[0] == 42);
    printf("  [PASS] test_single\n");
}

static void test_empty(void)
{
    uint8_t buf[16]; size_t enc_len = 99;
    assert(dod_encode(NULL, 0, buf, sizeof(buf), &enc_len) == STRIQ_OK);
    assert(enc_len == 0);
    printf("  [PASS] test_empty\n");
}

static void test_encode_indexed_regular(void)
{
    /* 512 regular timestamps spaced 1ms apart */
    int64_t ts[512];
    for (int i = 0; i < 512; i++) ts[i] = (int64_t)i * 1000000LL;

    uint8_t buf[8192];
    size_t enc_len = 0;
    striq_ts_index_entry_t idx[STRIQ_TS_INDEX_MAX_ENTRIES];
    uint16_t idx_count = 0;

    assert(dod_encode_indexed(ts, 512, buf, sizeof(buf),
                              &enc_len, idx, &idx_count) == STRIQ_OK);

    /* Expect ceil(512/256) = 2 index entries (rows 0, 256) */
    assert(idx_count == 2);
    assert(idx[0].row_offset == 0);
    assert(idx[0].ts == ts[0]);
    assert(idx[1].row_offset == 256);
    assert(idx[1].ts == ts[256]);

    /* Roundtrip via plain dod_decode */
    int64_t out[512];
    assert(dod_decode(buf, enc_len, 512, out) == STRIQ_OK);
    for (int i = 0; i < 512; i++) assert(out[i] == ts[i]);

    printf("  [PASS] encode_indexed_regular (idx_count=%u)\n", idx_count);
}

static void test_find_range(void)
{
    /* 1024 timestamps spaced 1ms */
    int64_t ts[1024];
    for (int i = 0; i < 1024; i++) ts[i] = (int64_t)i * 1000000LL;

    uint8_t buf[16384];
    size_t enc_len = 0;
    striq_ts_index_entry_t idx[STRIQ_TS_INDEX_MAX_ENTRIES];
    uint16_t idx_count = 0;
    assert(dod_encode_indexed(ts, 1024, buf, sizeof(buf),
                              &enc_len, idx, &idx_count) == STRIQ_OK);
    /* 4 entries: rows 0, 256, 512, 768 */
    assert(idx_count == 4);

    /* Query [ts[300], ts[700]] → should return start <= 300, end >= 700 */
    uint32_t start, end;
    assert(dod_find_range(idx, idx_count, 1024,
                          ts[300], ts[700], &start, &end) == STRIQ_OK);
    assert(start <= 300);
    assert(end   >= 700);
    assert(end   < 1024);

    /* No-filter query → full range */
    assert(dod_find_range(idx, idx_count, 1024,
                          0, 0, &start, &end) == STRIQ_OK);
    assert(start == 0);
    assert(end   == 1023);

    printf("  [PASS] find_range (start=%u end=%u for rows 300..700)\n",
           start, end);
}

static void test_decode_range(void)
{
    /* 512 timestamps */
    int64_t ts[512];
    for (int i = 0; i < 512; i++) ts[i] = (int64_t)i * 1000000LL + 5000LL;

    uint8_t buf[8192];
    size_t enc_len = 0;
    striq_ts_index_entry_t idx[STRIQ_TS_INDEX_MAX_ENTRIES];
    uint16_t idx_count = 0;
    assert(dod_encode_indexed(ts, 512, buf, sizeof(buf),
                              &enc_len, idx, &idx_count) == STRIQ_OK);

    /* Decode rows [100, 199] */
    int64_t out[100];
    assert(dod_decode_range(buf, enc_len, 512,
                            idx, idx_count,
                            100, 199, out) == STRIQ_OK);
    for (int i = 0; i < 100; i++)
        assert(out[i] == ts[100 + i]);

    /* Decode first and last rows */
    int64_t first[1], last[1];
    assert(dod_decode_range(buf, enc_len, 512, idx, idx_count, 0, 0, first) == STRIQ_OK);
    assert(first[0] == ts[0]);
    assert(dod_decode_range(buf, enc_len, 512, idx, idx_count, 511, 511, last) == STRIQ_OK);
    assert(last[0] == ts[511]);

    printf("  [PASS] decode_range (rows 100..199, first, last)\n");
}

int main(void)
{
    printf("=== test_dod ===\n");
    test_regular();
    test_irregular();
    test_negative();
    test_single();
    test_empty();
    test_encode_indexed_regular();
    test_find_range();
    test_decode_range();
    printf("All DoD tests passed.\n");
    return 0;
}
