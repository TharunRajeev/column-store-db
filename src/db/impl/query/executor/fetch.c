#include <string.h>

#include "client_context.h"
#include "query_exec.h"
#include "utils.h"

void exec_fetch(DbOperator *query, message *send_message) {
  cs165_log(stdout, "Executing fetch query.\n");
  FetchOperator *fetch_op = &query->operator_fields.fetch_operator;

  // Get the Result from the select handle
  Column *positions = get_handle(fetch_op->select_handle);
  if (!positions) {
    handle_error(send_message, "Invalid select handle\n");
    log_err("L%d in exec_fetch: %s\n", __LINE__, send_message->payload);
    return;
  }
  cs165_log(stdout, "exec_fetch: positions: %s\n", fetch_op->select_handle);

  // Get the Column to fetch from
  Column *fetch_col = fetch_op->col;
  if (!fetch_col) {
    handle_error(send_message, "Invalid column to fetch from\n");
    log_err("L%d in exec_fetch: %s\n", __LINE__, send_message->payload);
    return;
  }
  cs165_log(stdout, "exec_fetch: Fetching from column %s\n", fetch_col->name);

  // Create a new Result to store the fetched values
  Column *fetch_result;
  if (create_new_handle(fetch_op->fetch_handle, &fetch_result) != 0) {
    handle_error(send_message, "Failed to create new handle\n");
    log_err("L%d in exec_fetch: %s\n", __LINE__, send_message->payload);
    return;
  }
  fetch_result->data_type = fetch_col->data_type;
  fetch_result->num_elements = positions->num_elements;

  // get the size of a single element in the column
  if (fetch_col->data_type == INT) {
    fetch_result->data = malloc(fetch_result->num_elements * sizeof(int));
  } else {
    handle_error(send_message, "Fetching from non-integer column not supported\n");
    log_err("L%d in exec_fetch: %s\n", __LINE__, send_message->payload);
    return;
  }

  if (!fetch_result->data) {
    handle_error(send_message, "Failed to allocate memory for result data\n");
    log_err("L%d in exec_fetch: %s\n", __LINE__, send_message->payload);
    return;
  }

  if (positions->num_elements == 0) {
    send_message->status = OK_DONE;
    send_message->payload = "Done";
    send_message->length = strlen(send_message->payload);
    return;
  }

  //    Fetching the values
  //    -----------

  // Now we can start fetching the values. Since we have at least one element in the
  // select, we can initialize the min and max as below.
  fetch_result->min_value = fetch_col->max_value;
  fetch_result->max_value = fetch_col->min_value;
  fetch_result->sum = 0;

  log_info("exec_fetch: fetching from col %s\n", fetch_col->name);
  for (size_t i = 0; i < positions->num_elements; i++) {
    size_t index = ((int *)positions->data)[i];
    //   TODO: consider alternative implementation for fetching values, while updating
    //   stats. How can we optimize with batching, SIMD, etc?
    int value = ((int *)fetch_col->data)[index];
    cs165_log(stdout, "index= %zu, val= %d ", index, ((int *)fetch_col->data)[index]);
    ((int *)fetch_result->data)[i] = value;

    fetch_result->sum += value;
    if (value < fetch_result->min_value) {
      fetch_result->min_value = value;
    }
    if (value > fetch_result->max_value) {
      fetch_result->max_value = value;
    }
  }

  log_info("Fetch operation completed successfully.\n");
  send_message->status = OK_DONE;
  send_message->payload = "Done";
  send_message->length = strlen(send_message->payload);
  return;
}
