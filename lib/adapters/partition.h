#ifndef STRIQ_PARTITION_H
#define STRIQ_PARTITION_H

/*
 * partition — manages a directory of time-partitioned .striq files.
 *
 * Data flows:
 *   striq_partition_push()  →  active striq_store_t (warm+cold)
 *                          →  on rollover: finalize current store, update manifest
 *
 * Queries dispatch to the manifest (Level A) or open individual .striq files
 * through their file_provider + query engine (Level B).
 */

#include "manifest.h"
#include "store.h"

typedef struct striq_partition striq_partition_t;

typedef struct {
    char             dir_path[512];
    striq_period_t   period;
    striq_col_schema_t cols[STRIQ_MAX_COLS];
    uint32_t         num_cols;
    double           epsilon_b;
    /* Warm tier capacity passed down to the embedded store */
    uint32_t         warm_max_blocks;
    double           warm_max_memory_mb;
    /* Cold file max rows before rolling to next period partition */
    uint64_t         cold_rows_per_part;   /* 0 = unlimited */
} striq_partition_opts_t;

/* Create a new partition manager (creates dir_path if it does not exist).
 * If dir_path already contains a striq.manifest the existing data is loaded. */
striq_status_t striq_partition_open(
    striq_partition_t       **out,
    const striq_partition_opts_t *opts);

/* Flush all pending data, finalize the active partition, write manifest.
 * The partition object remains open for further pushes. */
striq_status_t striq_partition_sync(striq_partition_t *p);

/* Sync + free all resources. */
striq_status_t striq_partition_close(striq_partition_t *p);

/*
 * Push one row (ts + values[num_cols]) to the active partition.
 * If the timestamp would belong to a different period bucket, or if
 * cold_rows_per_part has been reached, a rollover is triggered first.
 */
striq_status_t striq_partition_push(
    striq_partition_t *p,
    int64_t            ts,
    const double      *values,
    uint32_t           num_values);

/*
 * Merge all cold partition files whose time range lies fully within
 * [ts_from, ts_to] into a single new .striq file, removing the originals.
 * Returns STRIQ_ERR_NOTIMPL until a full implementation is added.
 */
striq_status_t striq_partition_compact(
    striq_partition_t *p,
    int64_t ts_from,
    int64_t ts_to);

striq_status_t striq_partition_query_mean(
    striq_partition_t      *p,
    const char             *col_name,
    int64_t                 ts_from,
    int64_t                 ts_to,
    striq_query_result_t   *out);

striq_status_t striq_partition_query_count(
    striq_partition_t      *p,
    int64_t                 ts_from,
    int64_t                 ts_to,
    striq_query_result_t   *out);

striq_status_t striq_partition_query_min(
    striq_partition_t      *p,
    const char             *col_name,
    int64_t                 ts_from,
    int64_t                 ts_to,
    striq_query_result_t   *out);

striq_status_t striq_partition_query_max(
    striq_partition_t      *p,
    const char             *col_name,
    int64_t                 ts_from,
    int64_t                 ts_to,
    striq_query_result_t   *out);

uint32_t striq_partition_num_parts(const striq_partition_t *p);

#endif /* STRIQ_PARTITION_H */
