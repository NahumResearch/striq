#include "../../include/striq.h"
#include "../core/types.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

static int cmd_compress(int argc, char **argv)
{
    if (argc < 4) {
        fprintf(stderr, "Usage: striq compress <input.csv> <output.striq> [epsilon]\n");
        return 1;
    }
    const char *csv_path  = argv[2];
    const char *striq_path = argv[3];
    double eps = (argc >= 5) ? atof(argv[4]) : 0.0;

    FILE *fp = fopen(csv_path, "r");
    if (!fp) { fprintf(stderr, "Cannot open %s\n", csv_path); return 1; }

    char header[4096];
    if (!fgets(header, sizeof(header), fp)) {
        fclose(fp); fprintf(stderr, "Empty CSV\n"); return 1;
    }
    header[strcspn(header, "\r\n")] = '\0';

    char *col_names_buf[STRIQ_MAX_COLS + 1];
    char  header_copy[4096];
    strncpy(header_copy, header, sizeof(header_copy) - 1);
    header_copy[sizeof(header_copy)-1] = '\0';
    int nc = 0;
    char *tok = strtok(header_copy, ",");
    while (tok && nc <= (int)STRIQ_MAX_COLS) {
        col_names_buf[nc++] = tok;
        tok = strtok(NULL, ",");
    }
    if (nc < 2) {
        fclose(fp);
        fprintf(stderr, "CSV must have timestamp + at least 1 value column\n");
        return 1;
    }

    const char *val_names[STRIQ_MAX_COLS];
    striq_coltype_t val_types[STRIQ_MAX_COLS];
    int num_val_cols = nc - 1;
    for (int i = 0; i < num_val_cols; i++) {
        val_names[i] = col_names_buf[i + 1];
        val_types[i] = STRIQ_COL_FLOAT64;
    }

    striq_opts_t wopts = striq_opts_make(); wopts.epsilon = eps;
    striq_writer_t *w = striq_writer_open(
        striq_path, val_names, val_types, (size_t)num_val_cols, &wopts);
    if (!w) { fclose(fp); fprintf(stderr, "Cannot create %s\n", striq_path); return 1; }

    char line[65536];
    uint64_t rows = 0;
    double values[STRIQ_MAX_COLS];

    while (fgets(line, sizeof(line), fp)) {
        line[strcspn(line, "\r\n")] = '\0';
        if (line[0] == '\0') continue;
        char *p = line;
        char *end;
        int64_t ts = (int64_t)strtoll(p, &end, 10);
        if (end == p) continue;
        p = end;
        int vi = 0;
        while (*p == ',' && vi < num_val_cols) {
            p++;
            values[vi++] = strtod(p, &end);
            p = end;
        }
        if (vi != num_val_cols) continue;
        striq_writer_add_row(w, ts, values, (size_t)num_val_cols);
        rows++;
    }
    fclose(fp);

    striq_status_t s = striq_writer_close(w);
    if (s != STRIQ_OK) {
        fprintf(stderr, "Write error: %s\n", striq_status_str(s));
        return 1;
    }
    printf("Compressed %llu rows → %s\n", (unsigned long long)rows, striq_path);
    return 0;
}

static void parse_query_flags(int argc, char **argv,
                               int64_t *ts_from, int64_t *ts_to,
                               double  *threshold, striq_cmp_t *cmp,
                               int     *has_where)
{
    *ts_from   = 0;
    *ts_to     = 0;
    *has_where = 0;

    for (int i = 5; i < argc; i++) {
        if (strcmp(argv[i], "--from") == 0 && i + 1 < argc)
            *ts_from = (int64_t)strtoll(argv[++i], NULL, 10);
        else if (strcmp(argv[i], "--to") == 0 && i + 1 < argc)
            *ts_to = (int64_t)strtoll(argv[++i], NULL, 10);
        else if (strcmp(argv[i], "WHERE") == 0 && i + 3 < argc) {
            const char *op  = argv[i + 2];
            *threshold = strtod(argv[i + 3], NULL);
            if      (strcmp(op, ">")  == 0) *cmp = STRIQ_CMP_GT;
            else if (strcmp(op, ">=") == 0) *cmp = STRIQ_CMP_GTE;
            else if (strcmp(op, "<")  == 0) *cmp = STRIQ_CMP_LT;
            else if (strcmp(op, "<=") == 0) *cmp = STRIQ_CMP_LTE;
            else if (strcmp(op, "==") == 0) *cmp = STRIQ_CMP_EQ;
            else { fprintf(stderr, "Unknown operator '%s'\n", op); return; }
            *has_where = 1;
            i += 3;
        }
    }
}

static int parse_col_list(const char *arg, const char **out_names, int max)
{
    static char buf[4096];
    strncpy(buf, arg, sizeof(buf) - 1);
    buf[sizeof(buf) - 1] = '\0';
    int n = 0;
    char *tok = strtok(buf, ",");
    while (tok && n < max) {
        out_names[n++] = tok;
        tok = strtok(NULL, ",");
    }
    return n;
}

static int cmd_query(int argc, char **argv)
{
    if (argc < 4) {
        fprintf(stderr,
            "Usage: striq query <file.striq> <fn> <col> [--from N] [--to N] [WHERE col OP val]\n"
            "  fn: mean, count, sum, min, max, downsample, value_at, scan\n"
            "  col: column name, comma-separated list, or omit for all (value_at/scan)\n"
            "  OP: > >= < <= ==\n");
        return 1;
    }
    const char *path  = argv[2];
    const char *qtype = argv[3];
    const char *col_name = (argc >= 5) ? argv[4] : NULL;

    int64_t     ts_from = 0, ts_to = 0;
    double      threshold = 0.0;
    striq_cmp_t cmp = STRIQ_CMP_GT;
    int         has_where = 0;
    parse_query_flags(argc, argv, &ts_from, &ts_to, &threshold, &cmp, &has_where);

    striq_reader_t *r = striq_reader_open(path);
    if (!r) { fprintf(stderr, "Cannot open %s\n", path); return 1; }

    int ret = 0;

    if (strcmp(qtype, "mean") == 0) {
        striq_result_t res = {0};
        striq_status_t s;
        if (has_where)
            s = striq_query_mean_where(r, col_name, ts_from, ts_to, threshold, cmp, &res);
        else
            s = striq_query_mean(r, col_name, ts_from, ts_to, &res);
        if (s != STRIQ_OK) {
            fprintf(stderr, "Query error: %s\n", striq_status_str(s)); ret = 1;
        } else {
            printf("mean(%s) = %.6f ± %.6f  (%.2f%% data read, %llu rows)\n",
                col_name, res.value, res.error_bound, res.pct_data_read,
                (unsigned long long)res.rows_scanned);
        }
    } else if (strcmp(qtype, "count") == 0) {
        uint64_t cnt = 0;
        striq_status_t s = striq_query_count(r, ts_from, ts_to, &cnt);
        if (s != STRIQ_OK) {
            fprintf(stderr, "Query error: %s\n", striq_status_str(s)); ret = 1;
        } else {
            printf("count(%s) = %llu\n", col_name, (unsigned long long)cnt);
        }
    } else if (strcmp(qtype, "sum") == 0) {
        striq_result_t res = {0};
        striq_status_t s = striq_query_sum(r, col_name, ts_from, ts_to, &res);
        if (s != STRIQ_OK) {
            fprintf(stderr, "Query error: %s\n", striq_status_str(s)); ret = 1;
        } else {
            printf("sum(%s) = %.6f ± %.6f  (%llu rows)\n",
                col_name, res.value, res.error_bound,
                (unsigned long long)res.rows_scanned);
        }
    } else if (strcmp(qtype, "min") == 0) {
        striq_result_t res = {0};
        striq_status_t s = striq_query_min(r, col_name, ts_from, ts_to, &res);
        if (s != STRIQ_OK) {
            fprintf(stderr, "Query error: %s\n", striq_status_str(s)); ret = 1;
        } else {
            printf("min(%s) = %.6f ± %.6f\n",
                col_name, res.value, res.error_bound);
        }
    } else if (strcmp(qtype, "max") == 0) {
        striq_result_t res = {0};
        striq_status_t s = striq_query_max(r, col_name, ts_from, ts_to, &res);
        if (s != STRIQ_OK) {
            fprintf(stderr, "Query error: %s\n", striq_status_str(s)); ret = 1;
        } else {
            printf("max(%s) = %.6f ± %.6f\n",
                col_name, res.value, res.error_bound);
        }
    } else if (strcmp(qtype, "downsample") == 0) {
        /* downsample <file> downsample <col> <N> [--from T] [--to T] */
        uint32_t n_pts = (argc >= 6) ? (uint32_t)atoi(argv[5]) : 100u;
        if (n_pts == 0) n_pts = 100;

        double  *vals = (double  *)malloc(n_pts * sizeof(double));
        int64_t *tss  = (int64_t *)malloc(n_pts * sizeof(int64_t));
        if (!vals || !tss) {
            free(vals); free(tss);
            fprintf(stderr, "Out of memory\n"); ret = 1;
        } else {
            striq_status_t s = striq_query_downsample(
                r, col_name, ts_from, ts_to, n_pts, vals, tss);
            if (s != STRIQ_OK) {
                fprintf(stderr, "Downsample error: %s\n", striq_status_str(s)); ret = 1;
            } else {
                printf("# downsample %s n=%u\n", col_name, n_pts);
                printf("# ts_ns, value\n");
                for (uint32_t i = 0; i < n_pts; i++)
                    printf("%lld, %.10g\n", (long long)tss[i], vals[i]);
            }
            free(vals); free(tss);
        }
    } else if (strcmp(qtype, "value_at") == 0) {
        /*
         * striq query file.striq value_at [col,col,...] <timestamp>
         * If only a number is given (no col arg), query all columns.
         */
        int64_t ts = 0;
        const char **cnames = NULL;
        uint32_t ncols = 0;
        const char *col_ptrs[STRIQ_MAX_COLS];

        if (argc >= 6) {
            char *endp;
            int64_t maybe_ts = (int64_t)strtoll(argv[4], &endp, 10);
            if (*endp == '\0') {
                ts = maybe_ts;
            } else {
                ncols = (uint32_t)parse_col_list(argv[4], col_ptrs, STRIQ_MAX_COLS);
                cnames = col_ptrs;
                ts = (int64_t)strtoll(argv[5], NULL, 10);
            }
        } else if (argc == 5) {
            ts = (int64_t)strtoll(argv[4], NULL, 10);
        } else {
            fprintf(stderr, "Usage: striq query <file> value_at [cols] <timestamp>\n");
            striq_reader_close(r); return 1;
        }

        double values[STRIQ_MAX_COLS], errors[STRIQ_MAX_COLS];
        uint32_t got_cols = 0;
        striq_status_t s = striq_query_value_at(
            r, cnames, ncols, ts, values, errors, &got_cols);
        if (s != STRIQ_OK) {
            fprintf(stderr, "value_at error: %s\n", striq_status_str(s)); ret = 1;
        } else {
            striq_file_info_t info = {0};
            striq_inspect(path, &info);
            printf("timestamp = %lld\n", (long long)ts);
            for (uint32_t i = 0; i < got_cols; i++) {
                uint32_t ci = cnames ? i : i;
                const char *name = info.col_names[ci];
                if (cnames && i < ncols) name = cnames[i];
                if (errors[i] == 0.0)
                    printf("  %s = %.6f (exact)\n", name, values[i]);
                else
                    printf("  %s = %.6f (±%.6f)\n", name, values[i], errors[i]);
            }
        }
    } else if (strcmp(qtype, "scan") == 0) {
        /*
         * striq query file.striq scan [col,col,...] [--from N] [--to N]
         * If no col arg, scan all columns.
         */
        const char **cnames = NULL;
        uint32_t ncols = 0;
        const char *col_ptrs[STRIQ_MAX_COLS];

        if (col_name && col_name[0] != '-') {
            char *endp;
            (void)strtoll(col_name, &endp, 10);
            if (*endp != '\0') {
                ncols = (uint32_t)parse_col_list(col_name, col_ptrs, STRIQ_MAX_COLS);
                cnames = col_ptrs;
            }
        }

        int64_t sf = 0, st2 = 0;
        for (int i = 4; i < argc; i++) {
            if (strcmp(argv[i], "--from") == 0 && i + 1 < argc)
                sf = (int64_t)strtoll(argv[++i], NULL, 10);
            else if (strcmp(argv[i], "--to") == 0 && i + 1 < argc)
                st2 = (int64_t)strtoll(argv[++i], NULL, 10);
        }

        uint32_t cap = 10000;
        uint32_t got_cols = 0, got_rows = 0;

        striq_file_info_t info = {0};
        striq_inspect(path, &info);
        uint32_t actual_cols = cnames ? ncols : info.num_cols;

        double  *vals = malloc((size_t)cap * actual_cols * sizeof(double));
        int64_t *tss  = malloc((size_t)cap * sizeof(int64_t));
        if (!vals || !tss) {
            free(vals); free(tss);
            fprintf(stderr, "Out of memory\n");
            striq_reader_close(r); return 1;
        }

        striq_status_t s = striq_query_scan(
            r, cnames, ncols, sf, st2, vals, tss, cap, &got_rows, &got_cols);
        if (s != STRIQ_OK) {
            fprintf(stderr, "scan error: %s\n", striq_status_str(s)); ret = 1;
        } else {
            printf("timestamp");
            for (uint32_t c = 0; c < got_cols; c++) {
                const char *name = cnames ? cnames[c] : info.col_names[c];
                printf(",%s", name);
            }
            printf("\n");
            for (uint32_t row = 0; row < got_rows; row++) {
                printf("%lld", (long long)tss[row]);
                for (uint32_t c = 0; c < got_cols; c++)
                    printf(",%.10g", vals[row * got_cols + c]);
                printf("\n");
            }
        }
        free(vals); free(tss);
    } else {
        fprintf(stderr, "Unknown query type '%s'. Supported: mean, count, sum, min, max, downsample, value_at, scan\n", qtype);
        ret = 1;
    }

    striq_reader_close(r);
    return ret;
}

static const char *inspect_codec_name(uint8_t c)
{
    switch (c) {
        case 0: return "PLA_LINEAR";
        case 1: return "PLA_CHEB";
        case 3: return "RAW_STATS";
        case 5: return "DOD";
        case 6: return "RLE";
        case 7: return "DECIMAL";
        case 8: return "QUANT8";
        case 9: return "QUANT16";
        default: return "UNKNOWN";
    }
}

static int cmd_inspect(int argc, char **argv)
{
    if (argc < 3) {
        fprintf(stderr, "Usage: striq inspect <file.striq> [--blocks]\n");
        return 1;
    }
    int show_blocks = 0;
    for (int i = 3; i < argc; i++)
        if (strcmp(argv[i], "--blocks") == 0) show_blocks = 1;

    striq_file_info_t info = {0};
    striq_status_t s = striq_inspect(argv[2], &info);
    if (s != STRIQ_OK) {
        fprintf(stderr, "Inspect error: %s\n", striq_status_str(s));
        return 1;
    }
    printf("STRIQ file: %s\n", argv[2]);
    printf("  blocks    : %u\n",    info.num_blocks);
    printf("  rows      : %llu\n",  (unsigned long long)info.total_rows);
    printf("  columns   : %u\n",    info.num_cols);
    printf("  epsilon_b : %.6f\n",  info.epsilon_b);
    printf("  ts_min    : %lld\n",  (long long)info.ts_min);
    printf("  ts_max    : %lld\n",  (long long)info.ts_max);

    printf("\n  %-24s  %-12s  %s\n", "column", "codec", "~compressed");
    printf("  %-24s  %-12s  %s\n",   "------", "-----", "-----------");
    for (uint32_t c = 0; c < info.num_cols; c++) {
        printf("  %-24s  %-12s  %.1f KB\n",
               info.col_names[c],
               inspect_codec_name(info.col_codec[c]),
               info.col_compressed_bytes[c] / 1024.0);
    }

    if (show_blocks) {
        printf("\n  (per-block detail not yet exposed in public API)\n");
    }
    return 0;
}

static int cmd_verify(int argc, char **argv)
{
    if (argc < 3) {
        fprintf(stderr, "Usage: striq verify <file.striq>\n");
        return 1;
    }
    uint32_t checked = 0, corrupt = 0;
    striq_status_t s = striq_verify(argv[2], &checked, &corrupt);
    if (s != STRIQ_OK) {
        fprintf(stderr, "Verify error: %s\n", striq_status_str(s));
        return 1;
    }
    printf("Verified %s: %u blocks checked, %u corrupt\n",
           argv[2], checked, corrupt);
    return corrupt > 0 ? 1 : 0;
}

int main(int argc, char **argv)
{
    if (argc < 2) {
        fprintf(stderr,
            "Usage: striq <compress|query|inspect|verify> ...\n"
            "  compress  <in.csv> <out.striq> [epsilon]\n"
            "  query     <file> <fn> <col> [--from N] [--to N] [WHERE col OP val]\n"
            "    fn: mean, count, sum, min, max, variance, downsample, value_at, scan\n"
            "    value_at [col,col,...] <timestamp>   — point lookup (omit cols for all)\n"
            "    scan     [col,col,...] [--from] [--to] — range extract as CSV\n"
            "  inspect   <file.striq> [--blocks]\n"
            "  verify    <file.striq>\n");
        return 1;
    }
    if (strcmp(argv[1], "compress") == 0) return cmd_compress(argc, argv);
    if (strcmp(argv[1], "query")    == 0) return cmd_query(argc, argv);
    if (strcmp(argv[1], "inspect")  == 0) return cmd_inspect(argc, argv);
    if (strcmp(argv[1], "verify")   == 0) return cmd_verify(argc, argv);
    fprintf(stderr, "Unknown command '%s'\n", argv[1]);
    return 1;
}
