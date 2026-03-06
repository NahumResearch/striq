/*
 * demo_partition.c — demonstrates striq_partition_t multi-file partitioned storage
 *
 * Scenario:
 *   1. Create a day-partitioned store in /tmp/striq_demo_partition/
 *   2. Push 4 days × 1-hour of 1-second data (temperature + humidity)
 *   3. Query mean/min/max over the full range (Level B: opens each .striq file)
 *   4. Query Level A from the manifest (no file I/O)
 *   5. Print how many partition files were created
 */

#include <stdio.h>
#include <stdlib.h>
#include <math.h>
#include <assert.h>
#include "../lib/adapters/partition.h"

#define DEMO_DIR "/tmp/striq_demo_partition"

static void rmrf(const char *dir)
{
    char cmd[256];
    snprintf(cmd, sizeof(cmd), "rm -rf %s", dir);
    (void)system(cmd);
}

static double temp_v(int64_t ts_s) { return 20.0 + 5.0 * sin((double)ts_s * 0.001); }
static double hum_v (int64_t ts_s) { return 60.0 + 8.0 * cos((double)ts_s * 0.0007); }

int main(void)
{
    printf("=== STRIQ Partition Demo ===\n");
    rmrf(DEMO_DIR);

    striq_col_schema_t cols[2] = {
        { .name = "temp", .type = STRIQ_COL_FLOAT64, .codec = CODEC_PLA_LINEAR },
        { .name = "hum",  .type = STRIQ_COL_FLOAT64, .codec = CODEC_PLA_LINEAR },
    };

    striq_partition_opts_t opts = {
        .period             = PERIOD_DAY,
        .num_cols           = 2,
        .epsilon_b          = 0.05,
        .warm_max_blocks    = 1,   /* small warm ring forces eviction to cold */
        .warm_max_memory_mb = 1.0,
        .cold_rows_per_part = 0,   /* roll only on day boundary */
    };
    strncpy(opts.dir_path, DEMO_DIR, sizeof(opts.dir_path) - 1);
    opts.cols[0] = cols[0];
    opts.cols[1] = cols[1];

    striq_partition_t *part = NULL;
    assert(striq_partition_open(&part, &opts) == STRIQ_OK);

    /* Push 4 days × 3600 seconds */
    int64_t day_s  = 86400LL;
    int64_t day_ns = day_s * 1000000000LL;
    int start_day  = 100;
    int num_days   = 4;
    int secs_per_day = 5000; /* > 4096 (ENCODER_MAX_ROWS_PER_BLOCK) to ensure eviction */
    printf("Pushing %d days × %d rows = %d total rows (2 cols)...\n",
           num_days, secs_per_day, num_days * secs_per_day);

    for (int d = 0; d < num_days; d++) {
        int64_t day_base_s  = (int64_t)(start_day + d) * day_s;
        int64_t day_base_ns = (int64_t)(start_day + d) * day_ns;
        for (int i = 0; i < secs_per_day; i++) {
            int64_t ts = day_base_ns + (int64_t)i * 1000000000LL;
            int64_t ts_s = day_base_s + (int64_t)i;
            double vals[2] = { temp_v(ts_s), hum_v(ts_s) };
            assert(striq_partition_push(part, ts, vals, 2) == STRIQ_OK);
        }
    }

    printf("Querying mean/min/max across all partitions...\n");
    const char *col_names[2] = { "temp", "hum" };
    for (int c = 0; c < 2; c++) {
        striq_query_result_t mean = {0}, mn = {0}, mx = {0};
        striq_partition_query_mean(part, col_names[c], 0, 0, &mean);
        striq_partition_query_min (part, col_names[c], 0, 0, &mn);
        striq_partition_query_max (part, col_names[c], 0, 0, &mx);
        printf("  %-5s: mean=%.3f  min=%.3f  max=%.3f  (rows=%llu)\n",
               col_names[c], mean.value, mn.value, mx.value,
               (unsigned long long)mean.rows_scanned);
    }

    striq_query_result_t cnt = {0};
    striq_partition_query_count(part, 0, 0, &cnt);
    printf("  total rows counted = %.0f\n", cnt.value);

    /* Level A manifest query (no file I/O) */
    printf("\nLevel A manifest query (no file I/O, from saved manifest file)...\n");
    striq_partition_sync(part);  /* flush + save manifest */
    {
        static striq_manifest_t m_la;
        if (manifest_load(&m_la, DEMO_DIR) == STRIQ_OK && m_la.num_parts > 0) {
            double sum = 0.0; uint64_t c2 = 0;
            striq_status_t rc = manifest_query_sum(&m_la, "temp", 0, 0, &sum, &c2);
            if (rc == STRIQ_OK)
                printf("  manifest sum(temp) = %.2f  count = %llu\n",
                       sum, (unsigned long long)c2);
            else
                printf("  manifest query: %d\n", (int)rc);
        } else {
            printf("  manifest not yet persisted (data still in active store)\n");
        }
    }

    printf("\nPartition files (historical): %u\n", striq_partition_num_parts(part));

    assert(striq_partition_close(part) == STRIQ_OK);

    /* After close, the active partition is also in the manifest */
    static striq_manifest_t m;
    if (manifest_load(&m, DEMO_DIR) == STRIQ_OK)
        printf("Final manifest entries: %u (days of data)\n", m.num_parts);

    printf("\nDemo complete. Partition dir: %s\n", DEMO_DIR);
    return 0;
}
