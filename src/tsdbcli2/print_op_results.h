// Copyright (c) 2025 by Terry Greeniaus.
// All rights reserved.
#ifndef __SRC_TSDBCLI_PRINT_OP_RESULTS_H
#define __SRC_TSDBCLI_PRINT_OP_RESULTS_H

#include "parse_types.h"
#include <libtsdb/tsdb.h>

void print_op_results(const fields_list& fs, tsdb::select_op& op,
                      tsdb::wal_query& wq, size_t N);

#endif /* __SRC_TSDBCLI_PRINT_OP_RESULTS_H */
