#include "report.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

static const char *CSV_HEADER =
    "codec,dataset,column,N,raw_bytes,compressed_bytes,ratio,"
    "encode_ms,decode_ms,encode_mbs,decode_mbs,max_error,"
    "query_mean_us,query_ds_us,"
    "query_gorilla_us,query_lz4_us,query_zstd_us,"
    "query_min_us,query_max_us,query_sum_us,query_count_us,"
    "query_var_us,query_where_us,query_value_at_us,query_scan_us\n";

void report_write_csv(const char *path, const bench_results_t *rs)
{
    FILE *existing = fopen(path, "r");
    int needs_header = (existing == NULL);
    if (existing) fclose(existing);

    FILE *f = fopen(path, needs_header ? "w" : "a");
    if (!f) { fprintf(stderr, "report: cannot open '%s'\n", path); return; }

    if (needs_header) fputs(CSV_HEADER, f);

    for (int i = 0; i < rs->count; i++) {
        const bench_result_t *r = &rs->rows[i];
        fprintf(f,
            "%s,%s,%s,%u,%llu,%llu,%.4f,"
            "%.4f,%.4f,%.2f,%.2f,%.6f,"
            "%.2f,%.2f,"
            "%.2f,%.2f,%.2f,"
            "%.2f,%.2f,%.2f,%.2f,"
            "%.2f,%.2f,%.2f,%.2f\n",
            r->codec, r->dataset, r->column,
            r->N,
            (unsigned long long)r->raw_bytes,
            (unsigned long long)r->compressed_bytes,
            r->ratio,
            r->encode_ms, r->decode_ms,
            r->encode_mbs, r->decode_mbs,
            r->max_error,
            r->query_mean_us, r->query_ds_us,
            r->query_gorilla_us, r->query_lz4_us, r->query_zstd_us,
            r->query_min_us, r->query_max_us, r->query_sum_us,
            r->query_count_us,
            r->query_var_us, r->query_where_us,
            r->query_value_at_us, r->query_scan_us);
    }
    fclose(f);
}

void report_print_markdown(const bench_results_t *rs)
{
    printf("\n| Codec | Dataset | Column | N | Ratio | Enc MB/s | Mean µs | Min µs | Max µs | Sum µs | Count µs | Var µs | Where µs | DS µs | ValAt µs | Scan µs |\n");
    printf("|-------|---------|--------|---|-------|----------|---------|--------|--------|--------|----------|--------|----------|-------|----------|----------|\n");
    for (int i = 0; i < rs->count; i++) {
        const bench_result_t *r = &rs->rows[i];
        printf("| %-14s | %-20s | %-20s | %7u | %5.2fx | %8.1f | %7.1f | %6.1f | %6.1f | %6.1f | %8.1f | %6.1f | %8.1f | %5.1f | %8.1f | %8.1f |\n",
               r->codec, r->dataset, r->column,
               r->N, r->ratio,
               r->encode_mbs,
               r->query_mean_us, r->query_min_us, r->query_max_us,
               r->query_sum_us, r->query_count_us, r->query_var_us,
               r->query_where_us, r->query_ds_us,
               r->query_value_at_us, r->query_scan_us);
    }
    printf("\n");
}
