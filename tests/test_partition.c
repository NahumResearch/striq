#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>
#include <math.h>
#include <errno.h>
#ifdef _WIN32
#  include <direct.h>
#  include <sys/types.h>
#  include <sys/stat.h>
#  define test_mkdir(p) _mkdir(p)
#else
#  include <sys/stat.h>
#  define test_mkdir(p) mkdir(p, 0755)
#endif
#include "../lib/adapters/partition.h"

#define TMP_DIR  "striq_test_partition"
#define TMP_DIR2 "striq_test_part2"
#define TMP_DIR3 "striq_test_part3"
#define TMP_DIR4 "striq_test_part4"
#define TMP_DIR5 "striq_test_part5"
#define TMP_DIR6 "striq_test_part6"

static void rmrf_dir(const char *dir)
{
    char cmd[640];
#ifdef _WIN32
    snprintf(cmd, sizeof(cmd), "rmdir /s /q \"%s\" 2>NUL", dir);
#else
    snprintf(cmd, sizeof(cmd), "rm -rf %s", dir);
#endif
    (void)system(cmd);
}

static void test_manifest_roundtrip(void)
{
    rmrf_dir(TMP_DIR);
    assert(test_mkdir(TMP_DIR) == 0 || errno == EEXIST);

    const striq_col_schema_t cols[2] = {
        { .name = "temp", .type = STRIQ_COL_FLOAT64, .codec = CODEC_PLA_LINEAR },
        { .name = "hum",  .type = STRIQ_COL_FLOAT64, .codec = CODEC_PLA_LINEAR },
    };

    striq_manifest_t *m = calloc(1, sizeof(*m));
    assert(m);
    assert(manifest_init(m, TMP_DIR, PERIOD_DAY, cols, 2, 0.01) == STRIQ_OK);

    striq_manifest_entry_t e = {
        .path      = "part_d_100.striq",
        .ts_first  = 100LL * 86400LL * 1000000000LL,
        .ts_last   = 100LL * 86400LL * 1000000000LL + 1000LL,
        .num_rows  = 5000,
        .file_size = 12345,
    };
    e.col_stats[0] = (striq_col_stats_t){ .min=1.0, .max=5.0, .sum=3000.0, .count=5000 };
    e.col_stats[1] = (striq_col_stats_t){ .min=20.0, .max=80.0, .sum=150000.0, .count=5000 };

    assert(manifest_upsert(m, &e) == STRIQ_OK);
    assert(manifest_save(m) == STRIQ_OK);
    free(m);

    /* Reload */
    striq_manifest_t *m2 = calloc(1, sizeof(*m2));
    assert(m2);
    assert(manifest_load(m2, TMP_DIR) == STRIQ_OK);
    assert(m2->num_parts == 1);
    assert(m2->num_cols  == 2);
    assert(strncmp(m2->parts[0].path, "part_d_100.striq", 16) == 0);
    assert(m2->parts[0].num_rows == 5000);
    assert(fabs(m2->parts[0].col_stats[0].sum - 3000.0) < 1.0);
    assert(fabs(m2->parts[0].col_stats[1].min - 20.0) < 0.01);

    printf("  [PASS] manifest_roundtrip (parts=%u cols=%u)\n",
           m2->num_parts, m2->num_cols);
    free(m2);
    rmrf_dir(TMP_DIR);
}

static void test_push_and_query(void)
{
    rmrf_dir(TMP_DIR2);

    const striq_col_schema_t cols[1] = {
        { .name = "val", .type = STRIQ_COL_FLOAT64, .codec = CODEC_PLA_LINEAR },
    };
    striq_partition_opts_t opts = {
        .period           = PERIOD_DAY,
        .num_cols         = 1,
        .epsilon_b        = 0.001,
        .warm_max_blocks  = 8,
        .warm_max_memory_mb = 4.0,
        .cold_rows_per_part = 0,
    };
    strncpy(opts.dir_path, TMP_DIR2, sizeof(opts.dir_path) - 1);
    opts.cols[0] = cols[0];

    striq_partition_t *part = NULL;
    assert(striq_partition_open(&part, &opts) == STRIQ_OK);
    assert(part);

    uint32_t N = 3000;
    double bf_sum = 0.0;
    int64_t base_ts = 200LL * 86400LL * 1000000000LL;
    for (uint32_t i = 0; i < N; i++) {
        double v = (double)(i % 100) * 0.1;
        int64_t ts = base_ts + (int64_t)i * 1000000LL;
        assert(striq_partition_push(part, ts, &v, 1) == STRIQ_OK);
        bf_sum += v;
    }

    striq_query_result_t res;
    assert(striq_partition_query_mean(part, "val", 0, 0, &res) == STRIQ_OK);
    double true_mean = bf_sum / (double)N;
    assert(fabs(res.value - true_mean) <= 0.1 + 1e-4);
    assert(res.rows_scanned >= N);

    striq_query_result_t cres;
    assert(striq_partition_query_count(part, 0, 0, &cres) == STRIQ_OK);
    assert((uint64_t)cres.value >= N);

    printf("  [PASS] push_and_query (N=%u mean=%.4f expected=%.4f count=%.0f)\n",
           N, res.value, true_mean, cres.value);

    assert(striq_partition_close(part) == STRIQ_OK);
    rmrf_dir(TMP_DIR2);
}

static void test_rollover(void)
{
    rmrf_dir(TMP_DIR3);

    const striq_col_schema_t cols[1] = {
        { .name = "x", .type = STRIQ_COL_FLOAT64, .codec = CODEC_PLA_LINEAR },
    };
    striq_partition_opts_t opts = {
        .period           = PERIOD_DAY,
        .num_cols         = 1,
        .epsilon_b        = 0.5,
        .warm_max_blocks  = 4,
        .warm_max_memory_mb = 4.0,
        .cold_rows_per_part = 0,
    };
    strncpy(opts.dir_path, TMP_DIR3, sizeof(opts.dir_path) - 1);
    opts.cols[0] = cols[0];

    striq_partition_t *part = NULL;
    assert(striq_partition_open(&part, &opts) == STRIQ_OK);

    int64_t day_ns = 86400LL * 1000000000LL;
    uint32_t rows_per_day = 200;
    double v = 1.0;
    for (int day = 10; day < 13; day++) {
        int64_t base = (int64_t)day * day_ns;
        for (uint32_t i = 0; i < rows_per_day; i++) {
            int64_t ts = base + (int64_t)i * 1000000LL;
            assert(striq_partition_push(part, ts, &v, 1) == STRIQ_OK);
            v += 0.01;
        }
    }

    assert(striq_partition_close(part) == STRIQ_OK);

    striq_manifest_t *m = calloc(1, sizeof(*m));
    assert(m);
    assert(manifest_load(m, TMP_DIR3) == STRIQ_OK);
    assert(m->num_parts == 3);

    printf("  [PASS] rollover (num_parts=%u)\n", m->num_parts);
    free(m);
    rmrf_dir(TMP_DIR3);
}

static void test_rows_per_part_limit(void)
{
    rmrf_dir(TMP_DIR4);

    const striq_col_schema_t cols[1] = {
        { .name = "y", .type = STRIQ_COL_FLOAT64, .codec = CODEC_PLA_LINEAR },
    };
    striq_partition_opts_t opts = {
        .period             = PERIOD_DAY,
        .num_cols           = 1,
        .epsilon_b          = 0.5,
        .warm_max_blocks    = 4,
        .warm_max_memory_mb = 4.0,
        .cold_rows_per_part = 500,
    };
    strncpy(opts.dir_path, TMP_DIR4, sizeof(opts.dir_path) - 1);
    opts.cols[0] = cols[0];

    striq_partition_t *part = NULL;
    assert(striq_partition_open(&part, &opts) == STRIQ_OK);

    int64_t base_ts = 300LL * 86400LL * 1000000000LL;
    uint32_t N = 1200;
    for (uint32_t i = 0; i < N; i++) {
        double v = (double)i;
        int64_t ts = base_ts + (int64_t)i * 1000000LL;
        assert(striq_partition_push(part, ts, &v, 1) == STRIQ_OK);
    }

    assert(striq_partition_close(part) == STRIQ_OK);

    striq_manifest_t *m = calloc(1, sizeof(*m));
    assert(m);
    assert(manifest_load(m, TMP_DIR4) == STRIQ_OK);
    assert(m->num_parts >= 2);

    printf("  [PASS] rows_per_part_limit (num_parts=%u)\n", m->num_parts);
    free(m);
    rmrf_dir(TMP_DIR4);
}

static void test_compact_stub(void)
{
    rmrf_dir(TMP_DIR5);

    const striq_col_schema_t cols[1] = {
        { .name = "z", .type = STRIQ_COL_FLOAT64, .codec = CODEC_PLA_LINEAR },
    };
    striq_partition_opts_t opts = {
        .period   = PERIOD_DAY,
        .num_cols = 1,
        .epsilon_b = 0.1,
    };
    strncpy(opts.dir_path, TMP_DIR5, sizeof(opts.dir_path) - 1);
    opts.cols[0] = cols[0];

    striq_partition_t *part = NULL;
    assert(striq_partition_open(&part, &opts) == STRIQ_OK);

    assert(striq_partition_compact(part, 0, 0) == STRIQ_OK);

    printf("  [PASS] compact with 0 parts returns STRIQ_OK\n");
    assert(striq_partition_close(part) == STRIQ_OK);
    rmrf_dir(TMP_DIR5);
}

static void test_manifest_level_a_query(void)
{
    rmrf_dir(TMP_DIR6);
    assert(test_mkdir(TMP_DIR6) == 0 || errno == EEXIST);

    const striq_col_schema_t cols[1] = {
        { .name = "v", .type = STRIQ_COL_FLOAT64, .codec = CODEC_PLA_LINEAR },
    };

    striq_manifest_t *m = calloc(1, sizeof(*m));
    assert(m);
    assert(manifest_init(m, TMP_DIR6, PERIOD_DAY, cols, 1, 0.01) == STRIQ_OK);

    for (int i = 0; i < 3; i++) {
        striq_manifest_entry_t e;
        memset(&e, 0, sizeof(e));
        snprintf(e.path, sizeof(e.path), "part_d_%d.striq", i);
        e.ts_first  = (int64_t)i       * 86400LL * 1000000000LL;
        e.ts_last   = (int64_t)(i + 1) * 86400LL * 1000000000LL - 1;
        e.num_rows  = 1000;
        e.col_stats[0] = (striq_col_stats_t){
            .min=0.0, .max=10.0, .sum=5000.0 * (i + 1), .count=1000
        };
        assert(manifest_upsert(m, &e) == STRIQ_OK);
    }

    double sum = 0.0;
    uint64_t cnt = 0;
    assert(manifest_query_sum(m, "v", 0, 0, &sum, &cnt) == STRIQ_OK);
    assert(fabs(sum - 30000.0) < 1.0);
    assert(cnt == 3000);

    printf("  [PASS] manifest_level_a_query (sum=%.0f count=%llu)\n",
           sum, (unsigned long long)cnt);
    free(m);
    rmrf_dir(TMP_DIR6);
}

int main(void)
{
    setvbuf(stdout, NULL, _IONBF, 0);
    printf("=== test_partition ===\n");

    rmrf_dir(TMP_DIR); rmrf_dir(TMP_DIR2); rmrf_dir(TMP_DIR3);
    rmrf_dir(TMP_DIR4); rmrf_dir(TMP_DIR5); rmrf_dir(TMP_DIR6);

    test_manifest_roundtrip();
    test_push_and_query();
    test_rollover();
    test_rows_per_part_limit();
    test_compact_stub();
    test_manifest_level_a_query();

    rmrf_dir(TMP_DIR); rmrf_dir(TMP_DIR2); rmrf_dir(TMP_DIR3);
    rmrf_dir(TMP_DIR4); rmrf_dir(TMP_DIR5); rmrf_dir(TMP_DIR6);

    printf("=== All partition tests PASSED ===\n");
    return 0;
}
