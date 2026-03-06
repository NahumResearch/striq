#ifndef STRIQ_TYPES_H
#define STRIQ_TYPES_H
#include "../../include/striq.h"
#include <stdint.h>
#include <stddef.h>
#include <stdbool.h>


#define STRIQ_TRY(expr) \
    do { striq_status_t _s = (expr); if (_s != STRIQ_OK) return _s; } while (0)

typedef enum {
    CODEC_PLA_LINEAR = 0,   /* 18-byte segments: slope(8)+offset(8)+length(2) */
    CODEC_PLA_CHEB   = 1,   /* 34-byte segments: c0(8)+c1(8)+c2(8)+c3(8)+length(2) */
    CODEC_RAW_STATS  = 3,   /* 48-byte stats header + raw doubles; O(1) queries */
    CODEC_DOD        = 5,
    CODEC_RLE        = 6,   /* value-RLE for discrete/step signals (≤64 unique values) */
    CODEC_DECIMAL    = 7,   /* lossless: stats(48)+d_exp(1)+db(1)+base(8)+zigzag deltas */
    CODEC_QUANT8     = 8,   /* lossy ε-bounded: stats(48)+range(16)+N×uint8; 8x vs raw */
    CODEC_QUANT16    = 9,   /* lossy ε-bounded: stats(48)+range(16)+N×uint16; 4x vs raw */
} striq_codec_t;

/* Extract base codec (kept for decoder compat with any legacy codec bytes) */
#define CODEC_BASE(b) ((striq_codec_t)(b))

#define COL_FLOAT64  STRIQ_COL_FLOAT64

typedef struct {
    double   slope;
    double   offset;
    uint16_t length;  
} striq_segment_t;

typedef struct {
    char            name[64];
    striq_coltype_t type;
    striq_codec_t   codec;   
    double          epsilon_b;  
} striq_col_schema_t;

typedef struct {
    double   min;
    double   max;
    double   sum;
    uint64_t count;
    uint32_t num_segments;  
} striq_col_stats_t;

typedef struct {
    uint64_t file_offset;
    uint32_t block_size;
    uint32_t num_rows;
    int64_t  ts_first;
    int64_t  ts_last;
} striq_block_index_t;

typedef striq_result_t striq_query_result_t;

typedef striq_status_t (*striq_write_fn)(const uint8_t *data, size_t len, void *ctx);
typedef striq_status_t (*striq_read_fn)(uint8_t *buf, size_t len, void *ctx);
typedef striq_status_t (*striq_seek_fn)(int64_t offset, int whence, void *ctx);

typedef struct {
    uint8_t *buf;
    size_t   cap;
    size_t   used;
} striq_arena_t;

#define STRIQ_MAGIC            "STRIQ"
#define STRIQ_MAGIC_LEN        5
#define STRIQ_VERSION          3
#define STRIQ_VERSION_MIN      1   
#define STRIQ_HEADER_SIZE      64
#define STRIQ_DEFAULT_BLOCK_SIZE (1u << 20)  /* 1 MB */
#define STRIQ_ROUTE_SAMPLE     10000

#define STRIQ_MAX_BLOCKS       512


#define STRIQ_TS_INDEX_STEP    256u


#define STRIQ_TS_INDEX_MAX_ENTRIES 32u

typedef struct {
    int64_t  ts;
    uint32_t row_offset;
    uint32_t byte_offset;
} striq_ts_index_entry_t;

#endif 
