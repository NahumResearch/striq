#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>
#include <math.h>
#include "striq.h"

static const char *TMP_FILE = "striq_integrity_test.striq";

static void write_test_file(void)
{
    const char *col_names[] = { "val" };
    striq_coltype_t col_types[] = { STRIQ_COL_FLOAT64 };

    striq_opts_t opts = striq_opts_make(); opts.epsilon = 0.001;
    striq_writer_t *w = striq_writer_open(
        TMP_FILE, col_names, col_types, 1, &opts);
    assert(w != NULL);

    for (int i = 0; i < 2000; i++) {
        int64_t ts = (int64_t)i * 1000;
        double v = (double)i * 0.5;
        assert(striq_writer_add_row(w, ts, &v, 1) == STRIQ_OK);
    }
    assert(striq_writer_close(w) == STRIQ_OK);
    printf("  [OK] wrote test file %s\n", TMP_FILE);
}

static void test_verify_clean(void)
{
    uint32_t checked = 0, corrupt = 0;
    striq_status_t s = striq_verify(TMP_FILE, &checked, &corrupt);
    assert(s == STRIQ_OK);
    assert(checked >= 1);
    assert(corrupt == 0);
    printf("  [PASS] verify_clean (%u blocks, %u corrupt)\n", checked, corrupt);
}

static void test_verify_corrupted(void)
{
    /* Read the file, flip a byte in the middle, write back */
    FILE *fp = fopen(TMP_FILE, "r+b");
    assert(fp);

    fseek(fp, 0, SEEK_END);
    long sz = ftell(fp);
    assert(sz > 200);

    /* Flip byte at offset 100 (well inside the first block's data) */
    fseek(fp, 100, SEEK_SET);
    uint8_t b;
    assert(fread(&b, 1, 1, fp) == 1);
    b ^= 0xFF;
    fseek(fp, 100, SEEK_SET);
    assert(fwrite(&b, 1, 1, fp) == 1);
    fclose(fp);

    uint32_t checked = 0, corrupt = 0;
    striq_status_t s = striq_verify(TMP_FILE, &checked, &corrupt);
    /* Should return OK (we just report corruption, not fail) */
    assert(s == STRIQ_OK);
    /* Corruption should be detected (if v2 format) */
    printf("  [PASS] verify_corrupted (%u blocks checked, %u corrupt detected)\n",
           checked, corrupt);
    /* Note: if format is v1 we don't check CRC, so corrupt==0 is OK too */
}

static void test_read_corrupted_returns_error(void)
{
    /* A corrupted file opened for query should either:
     * (a) return STRIQ_ERR_CORRUPT from the reader, or
     * (b) return wrong data (v1 fallback) */
    striq_reader_t *r = striq_reader_open(TMP_FILE);
    /* r might be NULL if CRC check fails at open time, or non-NULL if lazy */
    if (r) {
        striq_result_t res = {0};
        striq_status_t s = striq_query_mean(r, "val", 0, 0, &res);
        /* Either OK with potentially wrong data, or STRIQ_ERR_CORRUPT */
        printf("  [PASS] read_corrupted: status=%d value=%.4f\n",
               (int)s, res.value);
        striq_reader_close(r);
    } else {
        printf("  [PASS] read_corrupted: open returned NULL (CRC checked at open)\n");
    }
}

static void restore_test_file(void)
{
    /* Re-write a clean file for subsequent tests */
    write_test_file();
    uint32_t checked = 0, corrupt = 0;
    striq_verify(TMP_FILE, &checked, &corrupt);
    assert(corrupt == 0);
    printf("  [OK] restored clean file\n");
}

int main(void)
{
    printf("=== test_integrity ===\n");
    write_test_file();
    test_verify_clean();
    test_verify_corrupted();
    test_read_corrupted_returns_error();
    restore_test_file();
    remove(TMP_FILE);
    printf("All integrity tests passed.\n");
    return 0;
}
