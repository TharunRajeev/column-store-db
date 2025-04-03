#include <limits.h>

#include "client_context.h"
#include "hash_table.h"
#include "optimizer.h"
#include "query_exec.h"
#include "utils.h"

// O(n * m) where n is the number of elements in psn1_col and m is the number of elements
// in psn2_col
void exec_nested_loop_join(Column *psn1_col, Column *psn2_col, Column *vals1_col,
                           Column *vals2_col, Column *resL, Column *resR);
void exec_naive_hash_join(Column *psn1_col, Column *psn2_col, Column *vals1_col,
                          Column *vals2_col, Column *resL, Column *resR);
void exec_grace_hash_join(Column *psn1_col, Column *psn2_col, Column *vals1_col,
                          Column *vals2_col, Column *resL, Column *resR);
void exec_hash_join(Column *psn1_col, Column *psn2_col, Column *vals1_col,
                    Column *vals2_col, Column *resL, Column *resR);

// just for experimenting on how using sorted index can improve the performance
void exec_sorted_idx_join(Column *psn1_col, Column *psn2_col, Column *vals1_col,
                          Column *vals2_col, Column *resL, Column *resR);

void exec_join(DbOperator *query, message *send_message) {
  JoinOperator join_op = query->operator_fields.join_operator;
  Column *psn1_col = join_op.posn1;
  Column *psn2_col = join_op.posn2;
  Column *vals1_col = join_op.vals1;
  Column *vals2_col = join_op.vals2;

  // Make column handles to store results
  Column *resL_col = NULL;
  Column *resR_col = NULL;

  if (create_new_handle(join_op.res_handle1, &resL_col) != 0 ||
      create_new_handle(join_op.res_handle2, &resR_col) != 0) {
    send_message->status = EXECUTION_ERROR;
    send_message->payload = "Failed to create result handles";
    send_message->length = strlen(send_message->payload);
    return;
  }

  resL_col->data_type = INT;
  resR_col->data_type = INT;
  resL_col->num_elements = 0;
  resR_col->num_elements = 0;

  switch (join_op.join_type) {
    case NESTED_LOOP:
      exec_nested_loop_join(psn1_col, psn2_col, vals1_col, vals2_col, resL_col, resR_col);
      break;
    case HASH:
      exec_hash_join(psn1_col, psn2_col, vals1_col, vals2_col, resL_col, resR_col);
      break;
    case GRACE_HASH:
      exec_grace_hash_join(psn1_col, psn2_col, vals1_col, vals2_col, resL_col, resR_col);
      break;
    case NAIVE_HASH:
      exec_naive_hash_join(psn1_col, psn2_col, vals1_col, vals2_col, resL_col, resR_col);
      break;
    default:
      send_message->status = EXECUTION_ERROR;
      send_message->payload = "Invalid join type";
      send_message->length = strlen(send_message->payload);
  }
}

/**
 * @brief Execute a join operation using a nested loop join algorithm.
 *
 * @param psn1_col
 * @param psn2_col
 * @param vals1_col
 * @param vals2_col
 * @param resL
 * @param resR
 */
void exec_nested_loop_join(Column *psn1_col, Column *psn2_col, Column *vals1_col,
                           Column *vals2_col, Column *resL, Column *resR) {
  log_debug("exec_nested_loop_join: executing nested loop join\n");
  size_t l_N = psn1_col->num_elements;
  size_t r_N = psn2_col->num_elements;

  int *l_psn = (int *)psn1_col->data;
  int *r_psn = (int *)psn2_col->data;

  int *l_vals = (int *)vals1_col->data;
  int *r_vals = (int *)vals2_col->data;

  size_t max_res_size = psn1_col->num_elements * psn2_col->num_elements;
  resL->data = malloc(sizeof(int) * max_res_size);
  resR->data = malloc(sizeof(int) * max_res_size);

  size_t k = 0;
  for (size_t i = 0; i < l_N; i++) {
    for (size_t j = 0; j < r_N; j++) {
      if (l_vals[i] == r_vals[j]) {
        ((int *)resL->data)[k] = l_psn[i];
        ((int *)resR->data)[k] = r_psn[j];
        k++;
      }
    }
  }
  resL->num_elements = k;
  resR->num_elements = k;
  log_info("exec_nested_loop_join: done\n");
}

void exec_naive_hash_join(Column *psn1_col, Column *psn2_col, Column *vals1_col,
                          Column *vals2_col, Column *resL, Column *resR) {
  log_debug("exec_hash_join: executing hash join\n");

  size_t l_N = psn1_col->num_elements;
  size_t r_N = psn2_col->num_elements;

  int *l_psn = (int *)psn1_col->data;
  int *r_psn = (int *)psn2_col->data;
  int *l_vals = (int *)vals1_col->data;
  int *r_vals = (int *)vals2_col->data;

  // First pass: Count matches to determine exact result size
  hashtable *ht = NULL;
  if (allocate(&ht, l_N) != 0) {
    log_err("exec_hash_join: failed to allocate hash table\n");
    return;
  }

  // Build phase: Insert all elements from left relation
  for (size_t i = 0; i < l_N; i++) {
    if (put(ht, l_vals[i], l_psn[i]) != 0) {
      log_err("exec_hash_join: failed to insert into hash table\n");
      deallocate(ht);
      return;
    }
  }

  // First probe phase: Count matches
  size_t total_matches = 0;
  int *matching_positions = malloc(sizeof(int) * l_N);
  int num_matches;

  if (!matching_positions) {
    log_err("exec_hash_join: failed to allocate matching positions buffer\n");
    deallocate(ht);
    return;
  }

  for (size_t j = 0; j < r_N; j++) {
    if (get(ht, r_vals[j], matching_positions, l_N, &num_matches) == 0) {
      total_matches += num_matches;
    }
  }

  // Allocate exact space needed
  resL->data = malloc(sizeof(int) * total_matches);
  resR->data = malloc(sizeof(int) * total_matches);

  if (!resL->data || !resR->data) {
    log_err("exec_hash_join: failed to allocate result arrays\n");
    free(matching_positions);
    deallocate(ht);
    if (resL->data) free(resL->data);
    if (resR->data) free(resR->data);
    return;
  }

  // Second probe phase: Fill results
  size_t k = 0;
  for (size_t j = 0; j < r_N; j++) {
    if (get(ht, r_vals[j], matching_positions, l_N, &num_matches) == 0) {
      for (int m = 0; m < num_matches; m++) {
        ((int *)resL->data)[k] = matching_positions[m];
        ((int *)resR->data)[k] = r_psn[j];
        k++;
      }
    }
  }

  // Clean up
  free(matching_positions);
  deallocate(ht);

  resL->num_elements = k;
  resR->num_elements = k;

  log_info("exec_hash_join: done. Produced %zu results\n", k);
}

void exec_grace_hash_join(Column *psn1_col, Column *psn2_col, Column *vals1_col,
                          Column *vals2_col, Column *resL, Column *resR) {
  log_debug("exec_grace_hash_join: Not implemented; using naive hash join\n");
  exec_naive_hash_join(psn1_col, psn2_col, vals1_col, vals2_col, resL, resR);
}

void exec_hash_join(Column *psn1_col, Column *psn2_col, Column *vals1_col,
                    Column *vals2_col, Column *resL, Column *resR) {
  log_debug("exec_hash_join: Not implemented; using naive hash join\n");
  exec_naive_hash_join(psn1_col, psn2_col, vals1_col, vals2_col, resL, resR);
}

// TODO: experiment on how using sorted index can improve the performance
void exec_sorted_idx_join(Column *psn1_col, Column *psn2_col, Column *vals1_col,
                          Column *vals2_col, Column *resL, Column *resR) {
  //   First create indices on both values columns
  vals1_col->index = malloc(sizeof(ColumnIndex));
  vals2_col->index = malloc(sizeof(ColumnIndex));
  vals1_col->index->idx_type = SORTED_UNCLUSTERED;
  vals2_col->index->idx_type = SORTED_UNCLUSTERED;
  create_idx_on(vals1_col, NULL);
  create_idx_on(vals2_col, NULL);

  // Since they are both sorted, we can do one pass through the data
  // and find the matching values
  size_t l_N = vals1_col->num_elements;
  size_t r_N = vals2_col->num_elements;

  int *l_vals = (int *)vals1_col->index->sorted_data;
  int *l_original_idxs = (int *)vals1_col->index->positions;
  int *l_psn = (int *)psn1_col->data;

  int *r_vals = (int *)vals2_col->index->sorted_data;
  int *r_original_idxs = (int *)vals2_col->index->positions;
  int *r_psn = (int *)psn2_col->data;

  size_t max_res_size = vals1_col->num_elements * vals2_col->num_elements;
  resL->data = malloc(sizeof(int) * max_res_size);
  resR->data = malloc(sizeof(int) * max_res_size);

  size_t i = 0, j = 0, k = 0;
  while (i < l_N && j < r_N) {
    if (l_vals[i] == r_vals[j]) {
      // Use a temp pointer since we want the next match to start at `j` too, in case of
      // duplicates
      //   NOTE: This handling of duplicates is BADD... worst case, both values on left
      //   and
      // right are the same
      size_t temp_j = j;

      // As long as the right matching values are not done, keep recording matches
      while (temp_j < r_N && l_vals[i] == r_vals[temp_j]) {
        // get the orginal positions before applying indexes
        int i_idx = l_original_idxs[i], j_idx = r_original_idxs[temp_j];
        ((int *)resL->data)[k] = l_psn[i_idx];
        ((int *)resR->data)[k] = r_psn[j_idx];
        k++;

        // move on to the next right value
        temp_j++;
      }
      i++;
    } else if (l_vals[i] < r_vals[j]) {
      i++;
    } else {
      j++;
    }
  }
  resL->num_elements = k;
  resR->num_elements = k;
  log_info("exec_sorted_idx_join: done\n");
}
