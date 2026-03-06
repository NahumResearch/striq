#ifndef STRIQ_H
#define STRIQ_H

/*
 * STRIQ — STReaming Indexed Query compression
 * Public C11 API. This is the ONLY header consumers should include.
 */

#include <stdint.h>
#include <stddef.h>
#include <string.h>

#define STRIQ_VERSION_MAJOR 0
#define STRIQ_VERSION_MINOR 1
#define STRIQ_VERSION_PATCH 0
#define STRIQ_VERSION_STRING "0.1.0"

#define STRIQ_MAX_COLS 64

#ifdef __cplusplus
extern "C" {
#endif

/* ── Status ───────────────────────────────────────────────────────── */
typedef enum {
    STRIQ_OK             =  0,
    STRIQ_ERR_IO         = -1,
    STRIQ_ERR_FORMAT     = -2,
    STRIQ_ERR_MEMORY     = -3,
    STRIQ_ERR_PARAM      = -4,
    STRIQ_ERR_CODEC      = -5,
    STRIQ_ERR_QUERY      = -6,
    STRIQ_ERR_NOTFOUND   = -7,
    STRIQ_ERR_NOTIMPL    = -8,
    STRIQ_ERR_CORRUPT    = -9,   /* CRC32 mismatch on read */
    STRIQ_ERR_VERSION    = -10,  /* unsupported format version */
} striq_status_t;

const char *striq_status_str(striq_status_t s);

/* ── Column types ─────────────────────────────────────────────────── */
typedef enum {
    STRIQ_COL_FLOAT64 = 0,
    STRIQ_COL_FLOAT32 = 1,
    STRIQ_COL_INT64   = 2,
    STRIQ_COL_INT32   = 3,
} striq_coltype_t;

/* ── Query result ─────────────────────────────────────────────────── */
typedef struct {
    double   value;
    double   error_bound;
    uint64_t rows_scanned;
    double   pct_data_read;
    double   pct_algebraic;   /* % rows answered without decompression */
} striq_result_t;

/* ── Writer options ───────────────────────────────────────────────── */

/*
 * col_epsilon[i] == 0.0 means "use global epsilon for column i".
 * col_skip bit i set forces CODEC_RAW_STATS for column i regardless of epsilon.
 */
typedef struct {
    double   epsilon;                        /* global error bound; 0 = auto */
    double   col_epsilon[STRIQ_MAX_COLS];    /* per-column override; 0 = use global */
    uint32_t block_size;                     /* 0 = default 1 MB */
    uint64_t col_skip;                       /* bitmask: bit i set → force RAW_STATS for col i */
} striq_opts_t;

/* Set bit i in opts.col_skip to force column i to use RAW_STATS encoding. */
#define STRIQ_SKIP_COL(opts, i)  ((opts).col_skip |= (UINT64_C(1) << (uint64_t)(i)))

static inline striq_opts_t striq_opts_make(void) {
    striq_opts_t o;
    memset(&o, 0, sizeof(o));
    return o;
}

#define STRIQ_DEFAULTS striq_opts_make()


typedef struct striq_writer striq_writer_t;


striq_writer_t *striq_writer_open(
    const char            *path,
    const char * const    *col_names,
    const striq_coltype_t *col_types,
    size_t                 num_cols,
    const striq_opts_t    *opts   /* NULL = STRIQ_DEFAULTS */
);

striq_status_t striq_writer_add_row(
    striq_writer_t *w,
    int64_t         timestamp_ns,
    const double   *values,
    size_t          num_values
);

striq_status_t striq_writer_close(striq_writer_t *w);


typedef struct striq_reader striq_reader_t;

striq_reader_t *striq_reader_open(const char *path);
striq_status_t  striq_reader_close(striq_reader_t *r);


typedef enum {
    STRIQ_CMP_GT  = 0,
    STRIQ_CMP_GTE = 1,
    STRIQ_CMP_LT  = 2,
    STRIQ_CMP_LTE = 3,
    STRIQ_CMP_EQ  = 4,
} striq_cmp_t;


striq_status_t striq_query_mean(
    striq_reader_t *r,
    const char     *col_name,
    int64_t         ts_from,   /* 0 = no filter */
    int64_t         ts_to,     /* 0 = no filter */
    striq_result_t *out
);

striq_status_t striq_query_count(
    striq_reader_t *r,
    int64_t         ts_from,
    int64_t         ts_to,
    uint64_t       *out
);

striq_status_t striq_query_mean_where(
    striq_reader_t *r,
    const char     *col_name,
    int64_t         ts_from,
    int64_t         ts_to,
    double          threshold,
    striq_cmp_t     cmp,
    striq_result_t *out
);

striq_status_t striq_query_sum(
    striq_reader_t *r,
    const char     *col_name,
    int64_t         ts_from,
    int64_t         ts_to,
    striq_result_t *out
);

striq_status_t striq_query_min(
    striq_reader_t *r,
    const char     *col_name,
    int64_t         ts_from,
    int64_t         ts_to,
    striq_result_t *out
);

striq_status_t striq_query_max(
    striq_reader_t *r,
    const char     *col_name,
    int64_t         ts_from,
    int64_t         ts_to,
    striq_result_t *out
);


striq_status_t striq_query_downsample(
    striq_reader_t *r,
    const char     *col_name,
    int64_t         ts_from,
    int64_t         ts_to,
    uint32_t        n_points,
    double         *out_values,
    int64_t        *out_ts
);

striq_status_t striq_query_variance(
    striq_reader_t *r,
    const char     *col_name,
    int64_t         ts_from,
    int64_t         ts_to,
    striq_result_t *out
);


striq_status_t striq_query_value_at(
    striq_reader_t     *r,
    const char * const *col_names,
    uint32_t            num_cols,
    int64_t             timestamp_ns,
    double             *out_values,
    double             *out_errors,
    uint32_t           *out_num_cols
);


striq_status_t striq_query_scan(
    striq_reader_t     *r,
    const char * const *col_names,
    uint32_t            num_cols,
    int64_t             ts_from,
    int64_t             ts_to,
    double             *out_values,
    int64_t            *out_timestamps,
    uint32_t            max_rows,
    uint32_t           *out_num_rows,
    uint32_t           *out_num_cols
);


typedef struct {
    uint32_t num_blocks;
    uint64_t total_rows;
    uint32_t num_cols;
    double   epsilon_b;
    int64_t  ts_min;
    int64_t  ts_max;
    char     col_names[STRIQ_MAX_COLS][64];  
    uint8_t  col_codec[STRIQ_MAX_COLS];      
    uint64_t col_compressed_bytes[STRIQ_MAX_COLS]; 
} striq_file_info_t;

striq_status_t striq_inspect(const char *path, striq_file_info_t *out);


striq_status_t striq_verify(
    const char *path,
    uint32_t   *num_blocks_checked,
    uint32_t   *num_blocks_corrupt
);

#ifdef __cplusplus
}
#endif

#endif /* STRIQ_H */
