#include "algorithms.h"

#include <stdio.h>
#include <stdlib.h>

#include "utils.h"

typedef struct {
  int value;     // Data value
  size_t index;  // Original position
} IndexedValue;

/**
 * @brief Comparator function for qsort, sorts by the `value` field in ascending order.
 */
int compare_fn(const void* a, const void* b) {
  const IndexedValue* ia = (const IndexedValue*)a;
  const IndexedValue* ib = (const IndexedValue*)b;
  return (ia->value > ib->value) - (ia->value < ib->value);  // Return 1, 0, or -1
}

/**
 * @brief Sorts the `data` in ascending order and keeps track of their original positions.
 *
 * The caller should be responsible for memory management of arrays: `data` and
 * `original_pos`. That is, allocating enough memory and free-ing it later.
 *
 * @param data Pointer to the array of integers to be sorted.
 * @param n_elements The number of elements in the `data` array.
 * @param original_pos Pointer to the array to store the original positions.
 * @return int Returns 0 on success, or -1 on error (e.g., NULL pointers).
 */
int sort(int* data, size_t n_elements, int* original_pos) {
  if (!data || !original_pos || n_elements == 0) {
    log_err("%Ld: sort: Invalid input; data=%p, original_pos=%p, n_elements=%zu\n",
            __LINE__, data, original_pos, n_elements);
    return -1;  // Error: Invalid input
  }

  // Create an auxiliary array to store values and their original indices
  IndexedValue* indexed_array = (IndexedValue*)malloc(n_elements * sizeof(IndexedValue));
  if (!indexed_array) {
    log_err("%Ld: sort: Failed to allocate memory for indexed_array\n", __LINE__);
    return -1;
  }

  // Populate the auxiliary array
  for (size_t i = 0; i < n_elements; i++) {
    indexed_array[i].value = data[i];
    indexed_array[i].index = i;
  }

  // Sort the auxiliary array using qsort
  qsort(indexed_array, n_elements, sizeof(IndexedValue), compare_fn);

  // Write sorted values back to `data` and store original positions
  for (size_t i = 0; i < n_elements; i++) {
    data[i] = indexed_array[i].value;
    original_pos[i] = indexed_array[i].index;
  }

  // Free the auxiliary array
  free(indexed_array);

  return 0;
}

size_t binary_search_right(int* sorted_data, size_t num_elements, int value) {
  // Binary search for sorted index
  size_t left = 0;
  size_t right = num_elements - 1;

  while (left < right) {
    size_t mid = left + (right - left) / 2;  // Floor division

    if (sorted_data[mid] <= value) {
      left = mid + 1;
    } else {
      right = mid;
    }
  }

  // Find rightmost position where value is strictly less than next element
  while (((size_t)left < num_elements - 1 && sorted_data[left] <= value)) {
    left++;
  }
  return left > 0 ? left - 1 : 0;
}

size_t binary_search_left(int* sorted_data, size_t num_elements, int value) {
  // Binary search for sorted index
  size_t left = 0;
  size_t right = num_elements - 1;

  while (left < right) {
    size_t mid = left + (right - left + 1) / 2;  // Ceiling division

    if (sorted_data[mid] <= value) {
      left = mid;
    } else {
      right = mid - 1;
    }
  }
  while (left > 0 && sorted_data[left] == value) {
    left--;
  }

  return sorted_data[left] < value ? left + 1 : left;
}
