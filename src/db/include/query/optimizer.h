#ifndef OPTIMIZER_H
#define OPTIMIZER_H

#include "operators.h"
#include "utils.h"

// For handling index creation
void create_idx_on(Column* col, message* send_message);
void cluster_idx_on(Table* table, Column* primary_col, message* send_message);

/**
 * @brief Uses `col->index` to return the index of a value in the column's data.
 *
 * Can return 3 values:
 *
 * - Index `i` s.t. `col->data[i - 1] < value < col->data[i+1]`
 *
 *  - or -1 if `value < min_value`
 *
 *  - or -2 if `value > max_value`
 *
 * @param col           Column with an index
 * @param value
 * @return ssize_t
 */
size_t idx_lookup_left(Column* column, int value);

size_t idx_lookup_right(Column* column, int value);

#endif /*  OPTIMIZER_H */
