#ifndef STRIQ_STORE_H
#define STRIQ_STORE_H

/*
 * striq_store — tiered storage: warm RAM ring buffer + cold .striq file.
 *
 * Ingest path:
 *   store_push(row) → accumulates rows → encoder → blocks
 *   block → memstore (warm ring)
 *   eviction → cold .striq file (append)
 *
 * Query path:
 *   multi_provider = cold file_provider ++ warm memstore_provider
 *   engine operates on multi_provider transparently
 *
 * store_close() calls store_sync() automatically.
 */

#include "../core/block_provider.h"
#include "../core/encoder.h"
#include "../core/query/engine.h"
#include "memstore.h"
#include "file_provider.h"
#include "file_io.h"
#include <stdio.h>

typedef struct striq_store striq_store_t;

typedef struct {
    double   epsilon_b;
    uint32_t warm_max_blocks;    /* 0 = 256 */
    uint32_t warm_max_memory_mb; /* 0 = 16 */
    char    *cold_path;          /* NULL = warm-only mode */
} striq_store_opts_t;

/* Create a new store. cold_path=NULL for warm-only. */
striq_store_t *striq_store_create(
    const char * const       *col_names,
    const striq_coltype_t    *col_types,
    uint32_t                  num_cols,
    const striq_store_opts_t *opts);

/* Open existing cold file and attach a fresh warm ring. */
striq_store_t *striq_store_open(
    const char               *cold_path,
    const striq_store_opts_t *opts);

/* Append one row. Blocks flush to warm automatically. */
striq_status_t striq_store_push(
    striq_store_t *s,
    int64_t        timestamp_ns,
    const double  *values,
    size_t         num_values);

/* Query (mean, count, sum, min, max) across warm+cold. */
striq_status_t striq_store_query_mean(
    striq_store_t *s, const char *col,
    int64_t ts_from, int64_t ts_to, striq_result_t *out);
striq_status_t striq_store_query_count(
    striq_store_t *s,
    int64_t ts_from, int64_t ts_to, uint64_t *out);
striq_status_t striq_store_query_min(
    striq_store_t *s, const char *col,
    int64_t ts_from, int64_t ts_to, striq_result_t *out);
striq_status_t striq_store_query_max(
    striq_store_t *s, const char *col,
    int64_t ts_from, int64_t ts_to, striq_result_t *out);

/* Flush unflushed warm blocks → cold file and rewrite footer. */
striq_status_t striq_store_sync(striq_store_t *s);

/* sync() + close + free. */
striq_status_t striq_store_close(striq_store_t *s);

#endif /* STRIQ_STORE_H */
