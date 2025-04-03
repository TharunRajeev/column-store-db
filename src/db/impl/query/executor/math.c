#include <limits.h>

#include "client_context.h"
#include "query_exec.h"
#include "utils.h"

void exec_aggr(DbOperator *query, message *send_message) {
  cs165_log(stdout, "Executing aggr query:\nres_handle: %s\ncol: %s\n",
            query->operator_fields.aggregate_operator.res_handle,
            query->operator_fields.aggregate_operator.col->name);

  // Get the Column from the fetch handle
  AggregateOperator *aggr_op = &query->operator_fields.aggregate_operator;
  Column *col = aggr_op->col;

  // Create a new Column to store the result
  Column *res_col;
  if (create_new_handle(aggr_op->res_handle, &res_col) != 0) {
    handle_error(send_message, "Failed to create new handle\n");
    log_err("L%d in handle_aggr: %s\n", __LINE__, send_message->payload);
  }
  cs165_log(stdout, "added new handle: %s\n", aggr_op->res_handle);

  if (query->type == AVG) {
    res_col->data = malloc(sizeof(double));
    *((double *)res_col->data) =
        col->num_elements == 0 ? 0.0 : (double)col->sum / col->num_elements;
    res_col->data_type = DOUBLE;
  } else if (query->type == MIN) {
    res_col->data = malloc(sizeof(long));
    *((long *)res_col->data) = col->min_value;
    res_col->data_type = LONG;
  } else if (query->type == MAX) {
    res_col->data = malloc(sizeof(long));
    *((long *)res_col->data) = col->max_value;
    res_col->data_type = LONG;
  } else if (query->type == SUM) {
    res_col->data = malloc(sizeof(long));
    *((long *)res_col->data) = col->sum;
    res_col->data_type = LONG;
  } else {
    handle_error(send_message, "Unsupported aggregate operation");
    log_err("L%d in handle_aggr: %s\n", __LINE__, send_message->payload);
  }
  res_col->num_elements = 1;
  send_message->status = OK_DONE;
  send_message->payload = "Done";
  send_message->length = strlen(send_message->payload);
}

void exec_arithmetic(DbOperator *query, message *send_message) {
  Column *col1 = query->operator_fields.arithmetic_operator.col1;
  Column *col2 = query->operator_fields.arithmetic_operator.col2;
  cs165_log(stdout, "Executing arithmetic on columns: %s, %s\n", col1->name, col2->name);

  //   We currently only support ADD and SUB over integer columns
  if (col1->data_type != INT || col2->data_type != INT) {
    handle_error(send_message,
                 "Arithmetic operations are only supported on integer columns");
    log_err("L%d in handle_arithmetic: %s\n", __LINE__, send_message->payload);
  }

  // Create a new Column to store the result
  Column *res_col;
  if (create_new_handle(query->operator_fields.arithmetic_operator.res_handle,
                        &res_col) != 0) {
    handle_error(send_message, "Failed to create new handle\n");
    log_err("L%d in handle_arithmetic: %s\n", __LINE__, send_message->payload);
  }

  res_col->data = malloc(col1->num_elements * sizeof(int));
  if (!res_col->data) {
    free(res_col);
    handle_error(send_message, "Failed to allocate memory for result data");
  }
  //   initialize stats
  res_col->min_value = INT_MAX;
  res_col->max_value = INT_MIN;
  res_col->sum = 0;

  // Perform the arithmetic operation
  if (query->type == ADD) {
    for (size_t i = 0; i < col1->num_elements; i++) {
      ((int *)res_col->data)[i] = ((int *)col1->data)[i] + ((int *)col2->data)[i];
      int val = ((int *)res_col->data)[i];

      res_col->sum += val;
      res_col->min_value = val < res_col->min_value ? val : res_col->min_value;
      res_col->max_value = val > res_col->max_value ? val : res_col->max_value;
    }
  } else if (query->type == SUB) {
    for (size_t i = 0; i < col1->num_elements; i++) {
      ((int *)res_col->data)[i] = ((int *)col1->data)[i] - ((int *)col2->data)[i];
      int val = ((int *)res_col->data)[i];
      // TODO: for maintainability, consider refactoring this in
      res_col->sum += val;
      res_col->min_value = val < res_col->min_value ? val : res_col->min_value;
      res_col->max_value = val > res_col->max_value ? val : res_col->max_value;
    }
  } else {
    handle_error(send_message, "Unsupported arithmetic operation");
    log_err("L%d in handle_arithmetic: %s\n", __LINE__, send_message->payload);
  }

  res_col->data_type = INT;
  res_col->num_elements = col1->num_elements;
  send_message->status = OK_DONE;
  send_message->payload = "Done";
  send_message->length = strlen(send_message->payload);
  log_info("Arithmetic operation completed for %s, with result stored in %s\n",
           col1->name, res_col->name);
}
