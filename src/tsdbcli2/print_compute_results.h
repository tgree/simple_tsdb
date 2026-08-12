// Copyright (c) 2026 by Terry Greeniaus.
// All rights reserved.
#ifndef __SRC_TSDBCLI_PRINT_COMPUTE_RESULTS_H
#define __SRC_TSDBCLI_PRINT_COMPUTE_RESULTS_H

#include <libtsdb/tsdb.h>
#include <shunter/shunting_yard.h>

void print_compute_results(tsdb::select_op& op,
                           tsdb::wal_query& wq,
                           shunting_yard::shunter& shunter,
                           size_t N);

#endif /* __SRC_TSDBCLI_PRINT_COMPUTE_RESULTS_H */
