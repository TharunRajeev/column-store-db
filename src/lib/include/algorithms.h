#ifndef ALGORITHMS_H
#define ALGORITHMS_H

#include <stddef.h>

size_t binary_search_left(int* sorted_data, size_t num_elements, int value);
size_t binary_search_right(int* sorted_data, size_t num_elements, int value);
/**
 * @brief sorts the `data` in ascending order and keeps track of their original positions.
 *
 * The caller should be responsible for memory management of arrays: `data` and
 * `original_pos`. That is, allocating enough memory and free-ing it later.
 *
 * @param data
 * @param original_pos
 * @param n_elements
 * @return int
 */
int sort(int* data, size_t n_elements, int* original_pos);
void test_sort(void);

#endif
