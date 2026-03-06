#ifndef STRIQ_CSV_LOADER_H
#define STRIQ_CSV_LOADER_H

/*
 * csv_loader.h — Load float columns from a CSV/TXT file.
 *
 * Features:
 *   - Auto-detects ',' vs ';' separator
 *   - Skips columns that don't parse as double (dates, strings)
 *   - Handles '?' as missing value (replaced with prev value or 0.0)
 *   - Skips blank lines and lines with fewer fields than header
 */

#include <stdint.h>
#include <stddef.h>

typedef struct {
    double   **columns;    /* columns[col_idx][row_idx] */
    char     **col_names;
    uint32_t   ncols;
    uint32_t   nrows;
} csv_dataset_t;

/*
 * Load a CSV/TXT file. Returns NULL on fatal error.
 * Caller must call csv_free() when done.
 */
csv_dataset_t *csv_load(const char *path);

void csv_free(csv_dataset_t *ds);

#endif /* STRIQ_CSV_LOADER_H */
