#ifndef STRIQ_REPORT_H
#define STRIQ_REPORT_H

/*
 * report.h — Write benchmark results as CSV files.
 */

#include "bench.h"

/*
 * Append results to a CSV file.
 * Creates the file with header row if it does not exist.
 */
void report_write_csv(const char *path, const bench_results_t *rs);

/*
 * Print a markdown summary table to stdout.
 */
void report_print_markdown(const bench_results_t *rs);

#endif /* STRIQ_REPORT_H */
