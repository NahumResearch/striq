#include "csv_loader.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <ctype.h>

#define MAX_COLS    256
#define MAX_LINE    65536
#define INIT_ROWS   65536

static char *trim(char *s)
{
    while (isspace((unsigned char)*s)) s++;
    char *e = s + strlen(s);
    while (e > s && isspace((unsigned char)*(e-1))) e--;
    *e = '\0';
    return s;
}

static int count_sep(const char *line, char sep)
{
    int n = 0;
    for (; *line; line++) if (*line == sep) n++;
    return n;
}

static int split(char *line, char sep, char **fields, int max_fields)
{
    int n = 0;
    char *p = line;
    while (n < max_fields) {
        fields[n++] = p;
        char *next = strchr(p, sep);
        if (!next) break;
        *next = '\0';
        p = next + 1;
    }
    return n;
}

csv_dataset_t *csv_load(const char *path)
{
    FILE *f = fopen(path, "r");
    if (!f) {
        fprintf(stderr, "csv_load: cannot open '%s': %s\n", path, strerror(errno));
        return NULL;
    }

    char *line = malloc(MAX_LINE);
    char *fields[MAX_COLS];
    if (!line) { fclose(f); return NULL; }

    if (!fgets(line, MAX_LINE, f)) {
        fclose(f); free(line); return NULL;
    }
    line[strcspn(line, "\r\n")] = '\0';

    /* Detect separator: prefer ',' but fall back to ';' */
    char sep = (count_sep(line, ',') >= count_sep(line, ';')) ? ',' : ';';

    char hdr_copy[MAX_LINE];
    strncpy(hdr_copy, line, MAX_LINE - 1);
    int nhdr = split(hdr_copy, sep, fields, MAX_COLS);
    long data_start = ftell(f);
    char probe[MAX_LINE];
    if (!fgets(probe, MAX_LINE, f)) {
        fclose(f); free(line); return NULL;
    }
    probe[strcspn(probe, "\r\n")] = '\0';

    char probe_copy[MAX_LINE];
    strncpy(probe_copy, probe, MAX_LINE - 1);
    char *pfields[MAX_COLS];
    int np = split(probe_copy, sep, pfields, MAX_COLS);
    if (np < nhdr) np = nhdr; /* some trailing fields may be empty */

    int col_map[MAX_COLS]; /* col_map[out_col] = hdr_col */
    int ncols = 0;
    for (int i = 0; i < nhdr && i < np; i++) {
        char *pf = trim(pfields[i]);
        if (strcmp(pf, "?") == 0 || strcmp(pf, "nan") == 0 || strcmp(pf, "NA") == 0) {
            col_map[ncols++] = i; /* numeric column with missing value */
            continue;
        }
        char *end;
        strtod(pf, &end);
        if (end != pf && *trim(end) == '\0') {
            col_map[ncols++] = i;
        }
    }

    if (ncols == 0) {
        fprintf(stderr, "csv_load: no numeric columns found in '%s'\n", path);
        fclose(f); free(line); return NULL;
    }

    /* Allocate output structure */
    csv_dataset_t *ds = calloc(1, sizeof(*ds));
    ds->ncols = (uint32_t)ncols;
    ds->col_names = calloc(ncols, sizeof(char*));
    ds->columns   = calloc(ncols, sizeof(double*));
    for (int i = 0; i < ncols; i++) {
        ds->col_names[i] = strdup(trim(fields[col_map[i]]));
        ds->columns[i]   = malloc(INIT_ROWS * sizeof(double));
    }

    uint32_t capacity = INIT_ROWS;
    uint32_t nrows = 0;

    char *pf_tmp[MAX_COLS];
    char probe2[MAX_LINE];
    strncpy(probe2, probe, MAX_LINE - 1);
    int npf = split(probe2, sep, pf_tmp, MAX_COLS);
    (void)npf;

    {
        for (int i = 0; i < ncols; i++) {
            char *fv = (col_map[i] < npf) ? trim(pf_tmp[col_map[i]]) : "0";
            double val = 0.0;
            if (strcmp(fv, "?") != 0 && strcmp(fv, "nan") != 0 && strcmp(fv, "NA") != 0) {
                char *end;
                val = strtod(fv, &end);
                if (end == fv) val = 0.0;
            }
            ds->columns[i][0] = val;
        }
        nrows = 1;
    }


    fseek(f, data_start, SEEK_SET);

    nrows = 0;
    double prev[MAX_COLS];
    memset(prev, 0, sizeof prev);

    while (fgets(line, MAX_LINE, f)) {
        line[strcspn(line, "\r\n")] = '\0';
        if (line[0] == '\0') continue;

        char *row_fields[MAX_COLS];
        int nf = split(line, sep, row_fields, MAX_COLS);

        if (nrows >= capacity) {
            capacity *= 2;
            for (int i = 0; i < ncols; i++) {
                ds->columns[i] = realloc(ds->columns[i], capacity * sizeof(double));
                if (!ds->columns[i]) { fclose(f); free(line); csv_free(ds); return NULL; }
            }
        }

        for (int i = 0; i < ncols; i++) {
            double val = prev[i];
            if (col_map[i] < nf) {
                char *fv = trim(row_fields[col_map[i]]);
                if (strcmp(fv, "?") != 0 && strcmp(fv, "nan") != 0 &&
                    strcmp(fv, "NA") != 0 && *fv != '\0') {
                    char *end;
                    double v = strtod(fv, &end);
                    if (end != fv) val = v;
                }
            }
            ds->columns[i][nrows] = val;
            prev[i] = val;
        }
        nrows++;
    }

    ds->nrows = nrows;
    fclose(f);
    free(line);
    return ds;
}

void csv_free(csv_dataset_t *ds)
{
    if (!ds) return;
    for (uint32_t i = 0; i < ds->ncols; i++) {
        free(ds->col_names[i]);
        free(ds->columns[i]);
    }
    free(ds->col_names);
    free(ds->columns);
    free(ds);
}
