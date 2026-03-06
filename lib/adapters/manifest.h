#ifndef STRIQ_MANIFEST_H
#define STRIQ_MANIFEST_H

/*
 * manifest — a simple text index for a partition directory.
 *
 * Format (one line per partition file):
 *   v1 <period> <num_cols> <col:type> ... eps=<epsilon>
 *   <relative_path> <ts_first> <ts_last> <num_rows> <file_size>
 *   ...
 *
 * Period tags: "hour", "day", "month"
 */

#include "../core/types.h"
#include <stdint.h>
#include <stddef.h>

#define MANIFEST_MAX_PARTS   8192u
#define MANIFEST_PATH_MAX     256u

typedef enum {
    PERIOD_HOUR  = 0,
    PERIOD_DAY   = 1,
    PERIOD_MONTH = 2,
} striq_period_t;

/* One entry per .striq partition file */
typedef struct {
    char     path[MANIFEST_PATH_MAX];   /* relative to manifest directory */
    int64_t  ts_first;
    int64_t  ts_last;
    uint64_t num_rows;
    uint64_t file_size;
    /* Per-column aggregate stats (Level A queries) */
    striq_col_stats_t col_stats[STRIQ_MAX_COLS];
} striq_manifest_entry_t;

typedef struct {
    char     dir_path[512];       /* directory containing manifest + .striq files */
    striq_period_t period;
    striq_col_schema_t cols[STRIQ_MAX_COLS];
    uint32_t num_cols;
    double   epsilon_b;

    striq_manifest_entry_t parts[MANIFEST_MAX_PARTS];
    uint32_t               num_parts;
} striq_manifest_t;

/* Create a new in-memory manifest (no file I/O) */
striq_status_t manifest_init(
    striq_manifest_t         *m,
    const char               *dir_path,
    striq_period_t            period,
    const striq_col_schema_t *cols,
    uint32_t                  num_cols,
    double                    epsilon_b);

/* Load manifest from file.  Path = dir_path/striq.manifest */
striq_status_t manifest_load(striq_manifest_t *m, const char *dir_path);

/* Save manifest to file (overwrites if exists) */
striq_status_t manifest_save(const striq_manifest_t *m);

/* Add or update a partition entry */
striq_status_t manifest_upsert(
    striq_manifest_t         *m,
    const striq_manifest_entry_t *entry);

/*
 * Aggregate stats from manifest only — microseconds for multi-year ranges.
 * For boundary partitions, returns STRIQ_ERR_NOTFOUND so the caller knows
 * to fall through to Level B (open .striq file).
 */
striq_status_t manifest_query_sum(
    const striq_manifest_t *m,
    const char *col_name,
    int64_t ts_from, int64_t ts_to,
    double *out_sum, uint64_t *out_count);

#endif /* STRIQ_MANIFEST_H */
