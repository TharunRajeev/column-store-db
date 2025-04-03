#include "handler.h"

#include <string.h>

#include "utils.h"
char *handle_print(DbOperator *query);
void handle_batched_queries(DbOperator *query, message *send_message);
void handle_dbOperator(DbOperator *query, message *send_message);

void handle_query(char *query, message *send_message, int client_socket,
                  ClientContext *client_context) {
  // 1. Parse command
  //    Query string is converted into a request for an database operator
  DbOperator *dbo = parse_command(query, send_message, client_socket, client_context);
  // 2. Handle request, if appropriate
  if (dbo) {
    // If batching queries, add to batch
    int should_batch_query =
        is_batch_queries_on(client_context) && !(dbo->type == EXEC_BATCH);
    if (should_batch_query && add_query_to_batch(dbo) == 0) {
      cs165_log(stdout, "Added query to batch\n");
      send_message->status = OK_DONE;
    } else {
      handle_dbOperator(dbo, send_message);
      db_operator_free(dbo);
    }
  }
}

/**
 * @brief execute_DbOperator
 * Executes a query and returns the result to be sent back to the client.
 *
 * @param query (DbOperator*): the query to execute on the database
 * @return char*
 */
void handle_dbOperator(DbOperator *query, message *send_message) {
  switch (query->type) {
    case CREATE:
    case CREATE_INDEX:
      exec_create(query, send_message);
      break;
    case SELECT: {
      double t0 = get_time();
      exec_select(query, send_message);
      log_client_perf(stdout, " t_exec = %.6fμs\n", get_time() - t0);
    } break;
    case FETCH:
      exec_fetch(query, send_message);
      break;
    case PRINT: {
      char *result = handle_print(query);
      if (!result) {
        handle_error(send_message, "Failed to print columns");
        return;
      }
      send_message->status = OK_DONE;
      send_message->length = strlen(result);
      send_message->payload = result;
    } break;
    case AVG:
    case MIN:
    case MAX:
    case SUM:
      exec_aggr(query, send_message);
      break;
    case ADD:
    case SUB:
      exec_arithmetic(query, send_message);
      break;
    case INSERT:
      exec_insert(query, send_message);
      break;
    case EXEC_BATCH: {
      // Currently supports only batch select queries, per milestone 2 requirements
      double t0 = get_time();
      handle_batched_queries(query, send_message);
      log_client_perf(stdout, "\thandle_batched_queries: t = %.6fμs\n", get_time() - t0);
      set_batch_queries(query->context, 0);
    } break;
    case JOIN:
      exec_join(query, send_message);
      break;
    default:
      cs165_log(stdout, "execute_DbOperator: Unknown query type\n");
      break;
  }
}

/**
 * @brief handle_batched_queries
 * Executes a batch of select queries in parallel
 *
 * @param query DbOperator containing the batched queries
 * @param send_message message to send back to the client
 */
void handle_batched_queries(DbOperator *query, message *send_message) {
  if (!query->context || !query->context->bselect_dbos) {
    log_err("handle_batched_queries: Invalid query\n");
    handle_error(send_message, "Invalid batched query");
  } else {
    handle_error(send_message,
                 "handle_batched_queries: invalid batched query execution request\nOnly "
                 "batched select queries are supported");
  }
  exec_batch_select(query, send_message);
}

/**
 * @brief Executes a print operation by constructing a string representation
 * of the columns in row-major format
 *
 * @param query DbOperator containing the print operation
 * @return char* Allocated string containing the formatted output
 */
char *handle_print(DbOperator *query) {
  cs165_log(stdout, "handle_print: starting\n");
  PrintOperator *print_op = &query->operator_fields.print_operator;
  if (!print_op || !print_op->columns || print_op->num_columns == 0) {
    log_err("L%d: handle_print failed. No columns to print\n", __LINE__);
    return NULL;
  }

  //   cs165_log(stdout, "handle_print: num_columns: %zu\n", print_op->num_columns);
  //   cs165_log(stdout, "handle_print: print_op->columns[0].name: %s\n",
  //             print_op->columns[0]->name);
  // All columns should have the same number of elements
  size_t num_rows = print_op->columns[0]->num_elements;

  // Calculate required buffer size (estimate)
  // Assume max 20 chars per number plus separator and newline
  size_t buffer_size = (num_rows * print_op->num_columns * 21) + 1;
  char *result = malloc(buffer_size);
  if (!result) return NULL;

  char *current = result;
  size_t remaining = buffer_size;

  //   cs165_log(stdout, "handle_print: scanning columns\n");
  // Print each row
  cs165_log(stdout, "handle_print: num_rows: %zu\n", num_rows);
  for (size_t row = 0; row < num_rows; row++) {
    // Print each column's value in the current row
    for (size_t col = 0; col < print_op->num_columns; col++) {
      //   cs165_log(stdout, "handle_print: col: %zu, row: %zu\n", col, row);
      Column *column = print_op->columns[col];
      size_t printed;

      // Print value based on data type
      if (column->data_type == INT) {
        int *data = (int *)column->data;
        printed = snprintf(current, remaining, "%d", data[row]);
      } else if (column->data_type == LONG) {
        long *data = (long *)column->data;
        printed = snprintf(current, remaining, "%ld", data[row]);
      } else if (column->data_type == DOUBLE) {
        double *data = (double *)column->data;
        printed = snprintf(current, remaining, "%f", data[row]);
      } else {
        log_err("handle_print: Unsupported data type\n");
        free(result);
        return NULL;
      }

      if (printed >= remaining) {
        free(result);
        return NULL;
      }

      current += printed;
      remaining -= printed;

      // Add separator (except for last column)
      if (col < print_op->num_columns - 1 && remaining > 1) {
        *current++ = ',';
        remaining--;
      }
    }

    // Add newline (except for last row)
    if (row < num_rows - 1 && remaining > 1) {
      *current++ = '\n';
      remaining--;
    }
  }
  *current = '\0';
  cs165_log(stdout, "handle_print: done\n");
  return result;
}

void set_batch_queries(ClientContext *client_context, int is_on) {
  if (!client_context) {
    log_err("set_batch_select: client context is not initialized\n");
    return;
  }
  client_context->is_batch_queries_on = is_on;
  log_info("set_batch_select: batch select is %s\n", is_on ? "ON" : "OFF");
}

int is_batch_queries_on(ClientContext *client_context) {
  if (!client_context) {
    log_err("is_batch_queries_on: client context is not initialized\n");
    return -1;
  }
  return client_context->is_batch_queries_on;
}

void db_operator_free_wrapper(void *ptr) { db_operator_free((DbOperator *)ptr); }

int add_query_to_batch(DbOperator *query) {
  log_info("add_query_to_batch: adding a query to batch\n");
  if (query->type != SELECT) {  // The caller (server.c) ensures query is Non-null
    log_err("add_query_to_batch: Support for non-SELECT queries not implemented yet\n");
    return -1;
  }

  ClientContext *context = query->context;

  if (!context->bselect_dbos) {
    context->bselect_dbos = vector_create(db_operator_free_wrapper);
    if (!context->bselect_dbos) {
      log_err("add_query_to_batch: failed to create batch queries vector\n");
      return -1;
    }
    //   } else {
    //     // Ensure the previous query's column is the same as current query's column
    //     // This is guaranteed from project requirements, so we can skip this check,
    //     until we
    //     // are extending for extra credit.
    //     DbOperator *prev_query = (DbOperator *)vector_get(
    //         context->bselect_dbos, vector_size(context->bselect_dbos) - 1);
    //     if (prev_query->operator_fields.select_operator.comparator->col !=
    //         query->operator_fields.select_operator.comparator->col) {
    //       log_err(
    //           "add_query_to_batch: previous select query's column is different from
    //           current " "query's column\n");
    //       return;
    //     }
  }

  vector_push_back(context->bselect_dbos, query);
  log_info("add_query_to_batch: added query to batch\n");
  return 0;
}
