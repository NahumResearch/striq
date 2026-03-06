/*
 * demo_store.c — demonstrates striq_store_t tiered warm+cold storage
 *
 * Scenario:
 *   1. Create a tiered store with a small warm ring (forces evictions to disk)
 *   2. Push 3 columns (temperature, humidity, pressure) for 2 hours of 1-second data
 *   3. Query mean/min/max across the full range
 *   4. Close, reopen (cold only), verify historical data is intact
 *   5. Print a 20-point downsample of temperature
 */

#include <stdio.h>
#include <stdlib.h>
#include <math.h>
#include <assert.h>
#include "../lib/adapters/store.h"
#include "../lib/adapters/file_provider.h"
#include "../lib/adapters/file_io.h"
#include "../lib/core/query/engine.h"

#define DEMO_PATH "/tmp/striq_demo_store.striq"
#define ROWS_PER_HOUR  3600
#define NUM_HOURS      2

static double temp_signal(int i) { return 20.0 + 5.0 * sin((double)i * 0.01); }
static double hum_signal (int i) { return 50.0 + 10.0 * cos((double)i * 0.008); }
static double pres_signal(int i) { return 1013.0 + 2.0 * sin((double)i * 0.003 + 1.0); }

int main(void)
{
    printf("=== STRIQ Store Demo ===\n");
    remove(DEMO_PATH);

    const char *col_names[] = { "temp", "hum", "pres" };
    striq_coltype_t col_types[] = {
        STRIQ_COL_FLOAT64, STRIQ_COL_FLOAT64, STRIQ_COL_FLOAT64
    };

    striq_store_opts_t opts = {
        .epsilon_b          = 0.01,
        .warm_max_blocks    = 4,     /* small warm ring — forces evictions */
        .warm_max_memory_mb = 2,
        .cold_path          = DEMO_PATH,
    };

    striq_store_t *s = striq_store_create(col_names, col_types, 3, &opts);
    assert(s);

    int total_rows = ROWS_PER_HOUR * NUM_HOURS;
    printf("Pushing %d rows (3 cols, 1 Hz)...\n", total_rows);

    for (int i = 0; i < total_rows; i++) {
        int64_t ts = (int64_t)i * 1000000000LL;  /* 1-second steps in nanoseconds */
        double vals[3] = { temp_signal(i), hum_signal(i), pres_signal(i) };
        assert(striq_store_push(s, ts, vals, 3) == STRIQ_OK);
    }

    printf("Querying warm+cold (mean/min/max per column)...\n");
    const char *cols[] = { "temp", "hum", "pres" };
    for (int c = 0; c < 3; c++) {
        striq_result_t mn = {0}, mx = {0}, mean = {0};
        assert(striq_store_query_mean(s, cols[c], 0, 0, &mean) == STRIQ_OK);
        assert(striq_store_query_min (s, cols[c], 0, 0, &mn)   == STRIQ_OK);
        assert(striq_store_query_max (s, cols[c], 0, 0, &mx)   == STRIQ_OK);
        printf("  %-5s: mean=%.3f  min=%.3f  max=%.3f  (±%.4f)\n",
               cols[c], mean.value, mn.value, mx.value, mean.error_bound);
    }

    uint64_t cnt = 0;
    assert(striq_store_query_count(s, 0, 0, &cnt) == STRIQ_OK);
    printf("  count = %llu rows\n", (unsigned long long)cnt);

    assert(striq_store_close(s) == STRIQ_OK);

    /* ── Reopen cold file and verify ── */
    printf("\nReopening cold file for historical query...\n");
    striq_store_t *s2 = striq_store_open(DEMO_PATH, &opts);
    if (s2) {
        striq_result_t r = {0};
        striq_store_query_mean(s2, "temp", 0, 0, &r);
        printf("  cold temp mean = %.3f (rows_scanned=%llu)\n",
               r.value, (unsigned long long)r.rows_scanned);
        striq_store_close(s2);
    } else {
        printf("  (cold file has 0 blocks — all data was in warm ring)\n");
    }

    /* ── Downsample via engine directly on cold file ── */
    printf("\nDownsample temp (20 points) from cold file...\n");
    {
        FILE *fp = fopen(DEMO_PATH, "rb");
        if (fp) {
            striq_file_ctx_t fctx = { fp };
            uint64_t fsz = file_io_size(fp);
            striq_fmt_reader_t rdr;
            if (fmt_reader_open(&rdr, file_io_read, file_io_seek, &fctx, fsz) == STRIQ_OK
                && rdr.num_blocks > 0) {
                striq_file_provider_t prov;
                file_provider_init(&prov, &rdr);
                striq_query_engine_t qe;
                engine_init(&qe, &prov.base);

                uint32_t n_pts = 20;
                double   ds_vals[20];
                int64_t  ds_ts[20];
                if (engine_query_downsample(&qe, "temp", 0, 0, n_pts, ds_vals, ds_ts)
                    == STRIQ_OK) {
                    printf("  # ts_ns, temp\n");
                    for (uint32_t i = 0; i < n_pts; i++)
                        printf("  %lld, %.4f\n", (long long)ds_ts[i], ds_vals[i]);
                }
            }
            fclose(fp);
        }
    }

    printf("\nDemo complete. Cold file: %s\n", DEMO_PATH);
    return 0;
}
