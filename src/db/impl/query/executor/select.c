#include <pthread.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include "client_context.h"
#include "handler.h"
#include "operators.h"
#include "optimizer.h"
#include "parse.h"
#include "query_exec.h"
#include "utils.h"
#include "vector.h"

#define BLOCK_SIZE 1024       // TODO: adjust based on L1 cache size
#define TEMP_BUFFER_SIZE 256  // Size for temporary results

// Define a structure to hold thread-specific results for each query
typedef struct {
  int **data;            // Array of result arrays, one per query
  size_t *num_elements;  // Array of element counts, one per query
} ThreadResultBuffer;

// Define a structure to pass data to threads
typedef struct {
  const int *data;
  size_t start_idx;
  size_t end_idx;
  Comparator **comparators;
  ThreadResultBuffer *thread_buffers;
  size_t num_queries;
} ThreadArgs;

// Function prototypes
size_t select_values_singlecore(const int *data, size_t num_elements,
                                Comparator *comparator, int *result_indices);

int batch_select_single_core(const int *data, size_t num_elements,
                             Comparator **comparators, Column **result_columns,
                             size_t num_queries);
int batch_select_single_core_optimized(const int *data, size_t num_elements,
                                       Comparator **comparators, Column **result_columns,
                                       size_t num_queries);
int batch_select_multi_core(const int *data, size_t num_elements,
                            Comparator **comparators, Column **result_columns,
                            size_t num_queries);

void double_probe_select(Column *column, Comparator *comparator, Column *result,
                         message *send_message);

/**
 * @brief exec_select
 * Executes a select query and returns the status of the query.
 * In doing so, it will update the global variable pool that's managed by the
 * client_context.c
 *
 *
 * @param query (DbOperator*): a DbOperator of type SELECT.
 * @return Status
 */
void exec_select(DbOperator *query, message *send_message) {
  SelectOperator *select_op = &query->operator_fields.select_operator;
  Comparator *comparator = select_op->comparator;
  Column *column = comparator->col;
  size_t n_elts = column->num_elements;
  int *data = (int *)column->data;

  // Create a new Column to store the result indices
  Column *result;
  if (create_new_handle(select_op->res_handle, &result) != 0) {
    log_err("exec_select: Failed to create new handle\n");
    send_message->status = EXECUTION_ERROR;
    send_message->length = 0;
    send_message->payload = NULL;
    return;
  }
  result->data_type = INT;  // Select returns an array of indices/integers

  // Allocate memory for the result data
  //   For simplicity, we will allocate the maximum possible size (for now).
  //   TODO: replace this with a dynamic array after implementing such a data structure.
  result->data = malloc(sizeof(int) * n_elts);
  if (!result->data) {
    log_err("exec_select: Failed to allocate memory for result data\n");
    send_message->status = EXECUTION_ERROR;
    send_message->length = 0;
    send_message->payload = NULL;
    return;
  }

  cs165_log(stdout, "exec_select: Starting to scan\n");

  // ref_posns is used to store the original positions of the data, this is used in
  // in the second type of `select()` query that receives results from fetch.
  //   Since indexes are on catalog columns, this `ref_posns` must always be NULL.
  int using_temp_ref_posns = 0;

  if (column->index && column->index->idx_type != NONE && !comparator->ref_posns) {
    //   Milestone 3: Index-based selection
    // double_probe_select(column, comparator, result, send_message);
    // return;

    // Get offset: where to start scanning based on the low value
    if (comparator->type1 == GREATER_THAN_OR_EQUAL &&
        comparator->p_low >= column->min_value) {
      // since low of query > min_value idx must be found
      size_t start_idx = idx_lookup_left(column, comparator->p_low);
      n_elts = column->num_elements - start_idx;
      data = column->index->sorted_data + start_idx;
      using_temp_ref_posns = 1;
      comparator->ref_posns = column->index->positions + start_idx;
      comparator->on_sorted_data = 1;
    }
  }

  if (n_elts < NUM_ELEMENTS_TO_MULTITHREAD || query->context->is_single_core) {
    //   Milestone 1 : Single - core selection: to avoid the overhead of creating
    //   threads
    result->num_elements =
        select_values_singlecore(data, n_elts, comparator, result->data);
    log_perf("\nqualifying range: [%ld, %ld]\n", comparator->p_low, comparator->p_high);
    log_perf("selectivity: %d/%zu = %.2f%%\n", result->num_elements, n_elts,
             (double)result->num_elements / n_elts * 100);
  } else {
    //   Milestone 2: Multi-core selection
    Comparator **comparators = malloc(sizeof(Comparator *));
    comparators[0] = comparator;
    Column **result_columns = malloc(sizeof(Column *));
    result_columns[0] = result;
    batch_select_multi_core(data, n_elts, comparators, result_columns, 1);
  }
  log_info("exec_select: Selection operation completed successfully.\n");

  //   Reset the temporary reference positions
  if (using_temp_ref_posns) comparator->ref_posns = NULL;

  //   set send_message
  send_message->status = OK_DONE;
  send_message->payload = "Done";
  send_message->length = strlen(send_message->payload);
  return;
}

void exec_batch_select(DbOperator *query, message *send_message) {
  Vector *batch_queries = query->context->bselect_dbos;
  if (!batch_queries || vector_size(batch_queries) == 0) {
    send_message->status = EXECUTION_ERROR;
    send_message->payload = "Empty batch queries";
    send_message->length = strlen(send_message->payload);
    return;
  }

  // Get the column from the first query to determine data size
  DbOperator *first_query = vector_get(batch_queries, 0);
  SelectOperator *first_select = &first_query->operator_fields.select_operator;
  Column *source_column = first_select->comparator->col;
  size_t num_elements = source_column->num_elements;

  // Create results for all queries in the batch
  size_t num_queries = vector_size(batch_queries);
  Column **result_columns = malloc(sizeof(Column *) * num_queries);
  Comparator **comparators = malloc(sizeof(Comparator *) * num_queries);
  if (!result_columns || !comparators) {
    log_err(
        "exec_batch_select: Memory allocation for result columns or comparators "
        "failed\n");
    send_message->status = EXECUTION_ERROR;
    send_message->payload = "Memory allocation failed";
    send_message->length = strlen(send_message->payload);
    return;
  }

  // Initialize result columns and comparators
  for (size_t i = 0; i < num_queries; i++) {
    DbOperator *curr_query = vector_get(batch_queries, i);
    SelectOperator *select_op = &curr_query->operator_fields.select_operator;

    result_columns[i] = NULL;
    if (create_new_handle(select_op->res_handle, &result_columns[i]) != 0) {
      // Clean up previously allocated columns
      for (size_t j = 0; j < i; j++) {
        free(result_columns[j]->data);
        free(result_columns[j]);
      }
      free(result_columns);

      send_message->status = EXECUTION_ERROR;
      send_message->payload = "Failed to create result handle";
      send_message->length = strlen(send_message->payload);
      return;
    }

    result_columns[i]->data_type = INT;
    result_columns[i]->data = malloc(sizeof(int) * num_elements);
    result_columns[i]->num_elements = 0;

    comparators[i] = select_op->comparator;

    if (!result_columns[i]->data) {
      // Clean up on failure
      for (size_t j = 0; j <= i; j++) {
        free(result_columns[j]->data);
        free(result_columns[j]);
      }
      free(result_columns);

      send_message->status = EXECUTION_ERROR;
      send_message->payload = "Memory allocation failed";
      send_message->length = strlen(send_message->payload);
      return;
    }
  }

  if (query->context->is_single_core) {
    batch_select_single_core((int *)source_column->data, num_elements, comparators,
                             result_columns, num_queries);
  } else {
    batch_select_multi_core((int *)source_column->data, num_elements, comparators,
                            result_columns, num_queries);
  }
  // Clean up and set success message
  free(result_columns);
  vector_destroy(batch_queries);
  query->context->bselect_dbos = NULL;

  send_message->status = OK_DONE;
  send_message->payload = "Batch select completed";
  send_message->length = strlen(send_message->payload);
}

// Basic initial comparison function using switch-case
int compare(ComparatorType type, long int p, int value) {
  switch (type) {
    case LESS_THAN:
      return value < p;
    case GREATER_THAN:
      return value > p;
    case EQUAL:
      return value == p;
    case LESS_THAN_OR_EQUAL:
      return value <= p;
    case GREATER_THAN_OR_EQUAL:
      return value >= p;
    default:
      return 0;
  }
}

// Evaluate a comparator against a single value
bool should_include(int value, Comparator *comparator) {
  ComparatorType type1 = comparator->type1;
  ComparatorType type2 = comparator->type2;
  long int p_low = comparator->p_low;
  long int p_high = comparator->p_high;

  // for select(..., null, p_high)
  if (type1 == NO_COMPARISON && type2 != NO_COMPARISON && compare(type2, p_high, value))
    return true;

  //   for select(..., p_low, null)
  if (type2 == NO_COMPARISON && type1 != NO_COMPARISON && compare(type1, p_low, value))
    return true;

  //   for select(..., p_low, p_high)
  if (type1 == GREATER_THAN_OR_EQUAL && type2 == LESS_THAN &&
      compare(type1, p_low, value) && compare(type2, p_high, value))
    return true;

  return false;
}

// Basic selection function (initial version)
size_t select_values_singlecore(const int *data, size_t num_elements,
                                Comparator *comparator, int *result_indices) {
  if (result_indices == NULL) {
    log_err("select_values_basic: result_indices is NULL\n");
    return -1;
  }
  int *ref_posns = comparator->ref_posns;
  int can_terminate = comparator->on_sorted_data && comparator->p_high;

  size_t result_count = 0;
  for (size_t i = 0; i < num_elements; i += 1) {
    if (can_terminate && comparator->p_high < data[i]) break;
    if (should_include(data[i], comparator)) {
      result_indices[result_count++] = ref_posns ? (size_t)ref_posns[i] : i;
    }
  }
  log_info("select_values_basic: Found %zu matching elements out of %zu\n", result_count,
           num_elements);
  return result_count;
}

int batch_select_single_core(const int *data, size_t num_elements,
                             Comparator **comparators, Column **result_columns,
                             size_t num_queries) {
  //   if (!data || !comparators || !result_columns) {
  //     log_err("batch_select_with_one_pass: Invalid input\n");
  //     return -1;
  //   }
  //   // Single pass through the data
  //   for (size_t i = 0; i < num_elements; i++) {
  //     int current_value = data[i];

  //     // Check against each query's comparator
  //     for (size_t q = 0; q < num_queries; q++) {
  //       Comparator *comparator = comparators[q];

  //       if (should_include(current_value, comparator)) {
  //         int *result_data = (int *)result_columns[q]->data;
  //         size_t curr_idx = result_columns[q]->num_elements;

  //         // Store either the reference position or the current index
  //         result_data[curr_idx] =
  //             comparator->ref_posns ? (size_t)comparator->ref_posns[i] : i;

  //         result_columns[q]->num_elements++;
  //       }
  //     }
  //   }
  //   return 0;

  // use optimized version
  return batch_select_single_core_optimized(data, num_elements, comparators,
                                            result_columns, num_queries);
}

// Worker function for each thread
void *thread_worker(void *args) {
  ThreadArgs *thread_args = (ThreadArgs *)args;
  const int *data = thread_args->data;
  size_t start_idx = thread_args->start_idx;
  size_t end_idx = thread_args->end_idx;
  Comparator **comparators = thread_args->comparators;
  ThreadResultBuffer *thread_buffers = thread_args->thread_buffers;
  size_t num_queries = thread_args->num_queries;

  // Initialize local buffers for this thread
  for (size_t q = 0; q < num_queries; q++) {
    thread_buffers->data[q] =
        malloc(sizeof(int) * (end_idx - start_idx));  // Over-allocate
    thread_buffers->num_elements[q] = 0;
  }

  for (size_t i = start_idx; i < end_idx; i++) {
    int current_value = data[i];

    for (size_t q = 0; q < num_queries; q++) {
      Comparator *comparator = comparators[q];

      if (should_include(current_value, comparator)) {
        int *result_data = thread_buffers->data[q];
        size_t curr_idx = thread_buffers->num_elements[q];

        result_data[curr_idx] =
            comparator->ref_posns ? (size_t)comparator->ref_posns[i] : i;

        thread_buffers->num_elements[q]++;
      }
    }
  }

  return NULL;
}

int batch_select_multi_core(const int *data, size_t num_elements,
                            Comparator **comparators, Column **result_columns,
                            size_t num_queries) {
  if (!data || !comparators || !result_columns) {
    log_err("batch_select_with_one_pass: Invalid input\n");
    return -1;
  }

  // Get the number of available cores
  size_t num_threads = sysconf(_SC_NPROCESSORS_ONLN);
  pthread_t threads[num_threads];
  ThreadArgs thread_args[num_threads];

  // Allocate thread-local buffers
  ThreadResultBuffer thread_buffers[num_threads];
  for (size_t t = 0; t < num_threads; t++) {
    thread_buffers[t].data = malloc(sizeof(int *) * num_queries);
    thread_buffers[t].num_elements = malloc(sizeof(size_t) * num_queries);
  }

  // Determine chunk size
  size_t chunk_size = (num_elements + num_threads - 1) / num_threads;

  // Create threads
  for (size_t t = 0; t < num_threads; t++) {
    thread_args[t].data = data;
    thread_args[t].start_idx = t * chunk_size;
    thread_args[t].end_idx =
        (t + 1) * chunk_size < num_elements ? (t + 1) * chunk_size : num_elements;
    thread_args[t].comparators = comparators;
    thread_args[t].thread_buffers = &thread_buffers[t];
    thread_args[t].num_queries = num_queries;

    if (pthread_create(&threads[t], NULL, thread_worker, &thread_args[t]) != 0) {
      log_err("Error creating thread %zu\n", t);
      return -1;
    }
  }

  // Wait for threads to complete
  for (size_t t = 0; t < num_threads; t++) {
    pthread_join(threads[t], NULL);
  }

  // Merge thread-local buffers into global result_columns
  for (size_t q = 0; q < num_queries; q++) {
    size_t total_elements = 0;

    // Calculate total elements for this query
    for (size_t t = 0; t < num_threads; t++) {
      total_elements += thread_buffers[t].num_elements[q];
    }

    // Allocate enough space in the global result_columns
    result_columns[q]->data =
        realloc(result_columns[q]->data, sizeof(int) * total_elements);

    // Copy data from each thread-local buffer
    size_t offset = 0;
    for (size_t t = 0; t < num_threads; t++) {
      size_t num_elements = thread_buffers[t].num_elements[q];
      memcpy((int *)result_columns[q]->data + offset, thread_buffers[t].data[q],
             sizeof(int) * num_elements);
      offset += num_elements;

      // Free thread-local buffer
      free(thread_buffers[t].data[q]);
    }

    log_perf("qualifying range: [%d, %d]\n", comparators[q]->p_low,
             comparators[q]->p_high);
    log_perf("selectivity: %d/%zu = %.2f%%\n", total_elements, num_elements,
             (double)total_elements / num_elements * 100);
    result_columns[q]->num_elements = total_elements;
  }

  // Free thread-local buffer metadata
  for (size_t t = 0; t < num_threads; t++) {
    free(thread_buffers[t].data);
    free(thread_buffers[t].num_elements);
  }

  return 0;
}

// Function to perform double-probe selection
void double_probe_select(Column *column, Comparator *comparator, Column *result,
                         message *send_message) {
  // Get offset: where to start scanning based on the low value
  if (comparator->p_high - 1 < column->min_value ||
      comparator->p_low > column->max_value) {
    //   if p_high is less than the min value, then no need to scan
    result->num_elements = 0;
    log_info("exec_select: No elements qualify the selection criteria\n");
    send_message->status = OK_DONE;
    send_message->payload = "Done";
    send_message->length = strlen(send_message->payload);
    return;
  }

  int has_low_and_high =
      comparator->type1 == GREATER_THAN_OR_EQUAL && comparator->type2 == LESS_THAN;
  if (has_low_and_high) {
    // since low of query > min_value idx must be found
    size_t start_idx = comparator->p_low <= column->min_value
                           ? 0
                           : idx_lookup_left(column, comparator->p_low);
    size_t end_idx = comparator->p_high >= column->max_value
                         ? column->num_elements - 1
                         : idx_lookup_right(column, comparator->p_high - 1);
    result->num_elements = end_idx - start_idx + 1;
    result->data = malloc(sizeof(int) * result->num_elements);
    memcpy(result->data, column->index->positions + start_idx,
           sizeof(int) * result->num_elements);
    log_info("p_low: %ld, p_high: %ld\n", comparator->p_low, comparator->p_high);
    log_info("idx suggests l,r where sorted_data[%ld] = %d, sorted_data[%ld] = %d\n",
             start_idx, column->index->sorted_data[start_idx], end_idx,
             column->index->sorted_data[end_idx]);
  }

  send_message->status = OK_DONE;
  send_message->payload = "Done";
  send_message->length = strlen(send_message->payload);
}

// Bitmap to track matches for each query
typedef struct {
  uint64_t bits[BLOCK_SIZE / 64];
} QueryBitmap;

static inline void clear_bitmap(QueryBitmap *bitmap) {
  memset(bitmap->bits, 0, sizeof(bitmap->bits));
}

// Efficiently mark matches in bitmap
static inline void mark_matches_bitmap(const int *block_data, size_t block_size,
                                       Comparator *comparator, QueryBitmap *bitmap) {
  for (size_t i = 0; i < block_size; i++) {
    int current_value = block_data[i];
    if (should_include(current_value, comparator)) {
      bitmap->bits[i / 64] |= (1ULL << (i % 64));
    }
  }
}

// Process matches using bitmap to reduce branching
static void process_matches(const size_t block_size, size_t base_idx,
                            const QueryBitmap *bitmap, const int *ref_posns,
                            int *result_data, size_t *result_count) {
  size_t temp_buffer[TEMP_BUFFER_SIZE];
  size_t temp_count = 0;

  // Process 64 bits at a time
  for (size_t i = 0; i < block_size / 64; i++) {
    uint64_t mask = bitmap->bits[i];
    while (mask) {
      // Find next set bit
      unsigned int bit_pos = __builtin_ctzll(mask);
      size_t idx = i * 64 + bit_pos;

      // Store in temporary buffer
      temp_buffer[temp_count++] =
          ref_posns ? (int)ref_posns[base_idx + idx] : (int)(base_idx + idx);

      // Clear processed bit
      mask &= mask - 1;

      // Flush temporary buffer if full
      if (temp_count == TEMP_BUFFER_SIZE) {
        memcpy(&result_data[*result_count], temp_buffer, temp_count * sizeof(int));
        *result_count += temp_count;
        temp_count = 0;
      }
    }
  }

  // Flush remaining results
  if (temp_count > 0) {
    memcpy(&result_data[*result_count], temp_buffer, temp_count * sizeof(int));
    *result_count += temp_count;
  }
}

int batch_select_single_core_optimized(const int *data, size_t num_elements,
                                       Comparator **comparators, Column **result_columns,
                                       size_t num_queries) {
  if (!data || !comparators || !result_columns) {
    return -1;
  }

  QueryBitmap *query_bitmaps = malloc(num_queries * sizeof(QueryBitmap));
  if (!query_bitmaps) return -1;

  // Process data in blocks
  for (size_t base_idx = 0; base_idx < num_elements; base_idx += BLOCK_SIZE) {
    size_t block_size =
        (num_elements - base_idx) < BLOCK_SIZE ? (num_elements - base_idx) : BLOCK_SIZE;
    const int *block_data = &data[base_idx];

    // First pass: Build bitmaps for all queries
    for (size_t q = 0; q < num_queries; q++) {
      clear_bitmap(&query_bitmaps[q]);
      mark_matches_bitmap(block_data, block_size, comparators[q], &query_bitmaps[q]);
    }

    // Second pass: Process matches for each query
    for (size_t q = 0; q < num_queries; q++) {
      int *result_data = (int *)result_columns[q]->data;
      process_matches(block_size, base_idx, &query_bitmaps[q], comparators[q]->ref_posns,
                      result_data, &result_columns[q]->num_elements);
    }
  }

  free(query_bitmaps);
  return 0;
}
