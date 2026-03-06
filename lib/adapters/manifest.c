#include "manifest.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define MANIFEST_FILENAME "striq.manifest"

striq_status_t manifest_init(
    striq_manifest_t         *m,
    const char               *dir_path,
    striq_period_t            period,
    const striq_col_schema_t *cols,
    uint32_t                  num_cols,
    double                    epsilon_b)
{
    if (!m || !dir_path || !cols || num_cols == 0) return STRIQ_ERR_PARAM;
    memset(m, 0, sizeof(*m));
    strncpy(m->dir_path, dir_path, sizeof(m->dir_path) - 1);
    m->period    = period;
    m->epsilon_b = epsilon_b;
    m->num_cols  = num_cols;
    memcpy(m->cols, cols, num_cols * sizeof(striq_col_schema_t));
    return STRIQ_OK;
}

static const char *period_str(striq_period_t p)
{
    switch (p) {
        case PERIOD_HOUR:  return "hour";
        case PERIOD_DAY:   return "day";
        case PERIOD_MONTH: return "month";
        default:           return "day";
    }
}

striq_status_t manifest_save(const striq_manifest_t *m)
{
    if (!m) return STRIQ_ERR_PARAM;

    char path[640];
    snprintf(path, sizeof(path), "%s/%s", m->dir_path, MANIFEST_FILENAME);

    FILE *fp = fopen(path, "w");
    if (!fp) return STRIQ_ERR_IO;

    fprintf(fp, "v1 %s %u", period_str(m->period), m->num_cols);
    for (uint32_t c = 0; c < m->num_cols; c++)
        fprintf(fp, " %s:f64", m->cols[c].name);
    fprintf(fp, " eps=%.10g\n", m->epsilon_b);

    for (uint32_t i = 0; i < m->num_parts; i++) {
        const striq_manifest_entry_t *e = &m->parts[i];
        fprintf(fp, "%s %lld %lld %llu %llu",
                e->path,
                (long long)e->ts_first,
                (long long)e->ts_last,
                (unsigned long long)e->num_rows,
                (unsigned long long)e->file_size);
        /* Per-col stats: min:max:sum:count */
        for (uint32_t c = 0; c < m->num_cols; c++) {
            const striq_col_stats_t *s = &e->col_stats[c];
            fprintf(fp, " %.17g:%.17g:%.17g:%llu",
                    s->min, s->max, s->sum,
                    (unsigned long long)s->count);
        }
        fprintf(fp, "\n");
    }

    fclose(fp);
    return STRIQ_OK;
}

striq_status_t manifest_load(striq_manifest_t *m, const char *dir_path)
{
    if (!m || !dir_path) return STRIQ_ERR_PARAM;
    memset(m, 0, sizeof(*m));
    strncpy(m->dir_path, dir_path, sizeof(m->dir_path) - 1);

    char path[640];
    snprintf(path, sizeof(path), "%s/%s", dir_path, MANIFEST_FILENAME);

    FILE *fp = fopen(path, "r");
    if (!fp) return STRIQ_ERR_IO;

    char line[4096];
    bool header_read = false;

    while (fgets(line, sizeof(line), fp)) {
        size_t len = strlen(line);
        if (len > 0 && line[len-1] == '\n') line[--len] = '\0';
        if (len == 0) continue;

        if (!header_read) {
            /* Parse header: v1 <period> <ncols> col:type... eps=X */
            char ver[8], period_tag[16];
            uint32_t nc = 0;
            if (sscanf(line, "%7s %15s %u", ver, period_tag, &nc) != 3) {
                fclose(fp); return STRIQ_ERR_FORMAT;
            }
            m->num_cols = nc > STRIQ_MAX_COLS ? STRIQ_MAX_COLS : nc;
            if (strcmp(period_tag, "hour")  == 0) m->period = PERIOD_HOUR;
            else if (strcmp(period_tag, "day") == 0) m->period = PERIOD_DAY;
            else m->period = PERIOD_MONTH;

            char *p = line;
            for (int t = 0; t < 3; t++) {
                while (*p && *p != ' ') p++;
                while (*p == ' ') p++;
            }
            for (uint32_t c = 0; c < m->num_cols; c++) {
                char tok[128] = {0};
                if (sscanf(p, "%127s", tok) != 1) break;
                char *colon = strchr(tok, ':');
                if (colon) *colon = '\0';
                if (strncmp(tok, "eps=", 4) == 0) break;
                snprintf(m->cols[c].name, sizeof(m->cols[c].name), "%s", tok);
                m->cols[c].type  = STRIQ_COL_FLOAT64;
                m->cols[c].codec = CODEC_PLA_LINEAR;
                while (*p && *p != ' ') p++;
                while (*p == ' ') p++;
            }
            char *eps_tok = strstr(line, "eps=");
            if (eps_tok) m->epsilon_b = atof(eps_tok + 4);
            header_read = true;
            continue;
        }

        if (m->num_parts >= MANIFEST_MAX_PARTS) break;
        striq_manifest_entry_t *e = &m->parts[m->num_parts];

        long long ts_first, ts_last;
        unsigned long long num_rows, file_size;
        if (sscanf(line, "%255s %lld %lld %llu %llu",
                   e->path, &ts_first, &ts_last, &num_rows, &file_size) != 5)
            continue;
        e->ts_first  = (int64_t)ts_first;
        e->ts_last   = (int64_t)ts_last;
        e->num_rows  = num_rows;
        e->file_size = file_size;

        char *p = line;
        for (int t = 0; t < 5; t++) {
            while (*p && *p != ' ') p++;
            while (*p == ' ') p++;
        }
        for (uint32_t c = 0; c < m->num_cols; c++) {
            double mn, mx, sm; unsigned long long cnt;
            if (sscanf(p, "%lg:%lg:%lg:%llu", &mn, &mx, &sm, &cnt) == 4) {
                e->col_stats[c].min   = mn;
                e->col_stats[c].max   = mx;
                e->col_stats[c].sum   = sm;
                e->col_stats[c].count = (uint64_t)cnt;
            }
            while (*p && *p != ' ') p++;
            while (*p == ' ') p++;
        }
        m->num_parts++;
    }

    fclose(fp);
    return STRIQ_OK;
}

striq_status_t manifest_upsert(
    striq_manifest_t         *m,
    const striq_manifest_entry_t *entry)
{
    if (!m || !entry) return STRIQ_ERR_PARAM;

    for (uint32_t i = 0; i < m->num_parts; i++) {
        if (strncmp(m->parts[i].path, entry->path, MANIFEST_PATH_MAX) == 0) {
            m->parts[i] = *entry;
            return STRIQ_OK;
        }
    }

    if (m->num_parts >= MANIFEST_MAX_PARTS) return STRIQ_ERR_MEMORY;
    m->parts[m->num_parts++] = *entry;
    return STRIQ_OK;
}

static int find_col_idx(const striq_manifest_t *m, const char *col_name)
{
    for (uint32_t c = 0; c < m->num_cols; c++) {
        if (strncmp(m->cols[c].name, col_name, 63) == 0) return (int)c;
    }
    return -1;
}

striq_status_t manifest_query_sum(
    const striq_manifest_t *m,
    const char *col_name,
    int64_t ts_from, int64_t ts_to,
    double *out_sum, uint64_t *out_count)
{
    if (!m || !col_name || !out_sum || !out_count) return STRIQ_ERR_PARAM;

    int ci = find_col_idx(m, col_name);
    if (ci < 0) return STRIQ_ERR_NOTFOUND;

    double   total_sum   = 0.0;
    uint64_t total_count = 0;
    bool     any         = false;

    for (uint32_t i = 0; i < m->num_parts; i++) {
        const striq_manifest_entry_t *e = &m->parts[i];
        if (ts_to   != 0 && e->ts_first > ts_to)   continue;
        if (ts_from != 0 && e->ts_last  < ts_from)  continue;
        total_sum   += e->col_stats[ci].sum;
        total_count += e->col_stats[ci].count;
        any = true;
    }

    if (!any) return STRIQ_ERR_NOTFOUND;
    *out_sum   = total_sum;
    *out_count = total_count;
    return STRIQ_OK;
}
