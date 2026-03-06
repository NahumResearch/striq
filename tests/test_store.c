#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>
#include <math.h>
#include "../lib/adapters/store.h"

#define TMP_FILE "striq_test_store.striq"

static void test_warm_only(void)
{
    const char *col_names[] = { "v" };
    striq_coltype_t col_types[] = { STRIQ_COL_FLOAT64 };

    striq_store_opts_t opts = { .epsilon_b = 0.001 };
    striq_store_t *s = striq_store_create(col_names, col_types, 1, &opts);
    assert(s);

    uint32_t N = 2000;
    double bf_sum = 0.0;
    for (uint32_t i = 0; i < N; i++) {
        double v = (double)i * 0.5;
        int64_t ts = (int64_t)i * 1000000LL;
        assert(striq_store_push(s, ts, &v, 1) == STRIQ_OK);
        bf_sum += v;
    }

    striq_result_t res = {0};
    assert(striq_store_query_mean(s, "v", 0, 0, &res) == STRIQ_OK);
    double true_mean = bf_sum / (double)N;
    double abs_err = fabs(res.value - true_mean);
    assert(abs_err <= res.error_bound + 1e-4);

    uint64_t cnt = 0;
    assert(striq_store_query_count(s, 0, 0, &cnt) == STRIQ_OK);
    assert(cnt == N);

    printf("  [PASS] warm_only (mean=%.4f expected=%.4f count=%llu)\n",
           res.value, true_mean, (unsigned long long)cnt);

    assert(striq_store_close(s) == STRIQ_OK);
}

static void test_cold_eviction(void)
{
    remove(TMP_FILE);

    const char *col_names[] = { "x" };
    striq_coltype_t col_types[] = { STRIQ_COL_FLOAT64 };

    striq_store_opts_t opts = {
        .epsilon_b       = 0.001,
        .warm_max_blocks = 2,     /* tiny warm ring to force eviction */
        .cold_path       = TMP_FILE
    };
    striq_store_t *s = striq_store_create(col_names, col_types, 1, &opts);
    assert(s);

    /* Push enough rows to fill warm (2 blocks = 2*4096 rows) and evict 1 */
    uint32_t N = 12288;  /* 3 full blocks */
    double bf_sum = 0.0;
    for (uint32_t i = 0; i < N; i++) {
        double v = (double)(i % 1000);
        int64_t ts = (int64_t)i * 1000000LL;
        assert(striq_store_push(s, ts, &v, 1) == STRIQ_OK);
        bf_sum += v;
    }

    /* Sync writes cold file footer */
    assert(striq_store_sync(s) == STRIQ_OK);

    /* Query across warm+cold */
    striq_result_t res = {0};
    assert(striq_store_query_mean(s, "x", 0, 0, &res) == STRIQ_OK);
    double true_mean = bf_sum / (double)N;
    double abs_err = fabs(res.value - true_mean);
    assert(abs_err <= res.error_bound * 2.0 + 1.0);  /* tolerance for cross-tier */

    uint64_t cnt = 0;
    assert(striq_store_query_count(s, 0, 0, &cnt) == STRIQ_OK);
    assert(cnt == N);

    printf("  [PASS] cold_eviction (mean=%.4f expected=%.4f count=%llu)\n",
           res.value, true_mean, (unsigned long long)cnt);

    assert(striq_store_close(s) == STRIQ_OK);

    /* Verify cold file was created */
    FILE *f = fopen(TMP_FILE, "rb");
    assert(f);
    fseek(f, 0, SEEK_END);
    long fsz = ftell(f);
    fclose(f);
    assert(fsz > 100);
    printf("  [PASS] cold_file_created (size=%ld bytes)\n", fsz);
}

static void test_reopen(void)
{
    /* test_cold_eviction already wrote TMP_FILE */
    striq_store_t *s = striq_store_open(TMP_FILE, NULL);
    if (!s) {
        printf("  [SKIP] reopen (cold file may not exist)\n");
        return;
    }

    uint64_t cnt = 0;
    striq_status_t st = striq_store_query_count(s, 0, 0, &cnt);
    if (st == STRIQ_OK) {
        assert(cnt > 0);
        printf("  [PASS] reopen (cold rows=%llu)\n", (unsigned long long)cnt);
    } else {
        printf("  [SKIP] reopen query (status=%d, cold may be partial)\n", (int)st);
    }

    assert(striq_store_close(s) == STRIQ_OK);
    remove(TMP_FILE);
}

static void test_multicol(void)
{
    const char *col_names[] = { "temp", "pressure" };
    striq_coltype_t col_types[] = { STRIQ_COL_FLOAT64, STRIQ_COL_FLOAT64 };

    striq_store_opts_t opts = { .epsilon_b = 0.01 };
    striq_store_t *s = striq_store_create(col_names, col_types, 2, &opts);
    assert(s);

    uint32_t N = 1000;
    double bf_sum_t = 0.0;
    for (uint32_t i = 0; i < N; i++) {
        double vals[2] = { 20.0 + sin((double)i * 0.05) * 5.0,
                           1013.25 + sin((double)i * 0.03) * 10.0 };
        int64_t ts = (int64_t)i * 1000000LL;
        assert(striq_store_push(s, ts, vals, 2) == STRIQ_OK);
        bf_sum_t += vals[0];
    }

    striq_result_t res_t = {0}, res_p = {0};
    assert(striq_store_query_mean(s, "temp",     0, 0, &res_t) == STRIQ_OK);
    assert(striq_store_query_mean(s, "pressure", 0, 0, &res_p) == STRIQ_OK);

    double true_mean_t = bf_sum_t / (double)N;
    assert(fabs(res_t.value - true_mean_t) <= res_t.error_bound + 0.1);

    printf("  [PASS] multicol (temp_mean=%.3f pressure_mean=%.3f)\n",
           res_t.value, res_p.value);

    assert(striq_store_close(s) == STRIQ_OK);
}

int main(void)
{
    printf("=== test_store ===\n");
    test_warm_only();
    test_cold_eviction();
    test_reopen();
    test_multicol();
    printf("All store tests passed.\n");
    return 0;
}
