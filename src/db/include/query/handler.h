#ifndef QUERY_HANDLER_H
#define QUERY_HANDLER_H

#include "client_context.h"
#include "operators.h"
#include "parse.h"
#include "query_exec.h"
#include "utils.h"

void handle_query(char *query, message *send_message, int client_socket,
                  ClientContext *client_context);
int is_batch_queries_on(ClientContext *client_context);
void set_batch_queries(ClientContext *client_context, int is_on);
int add_query_to_batch(DbOperator *query);

#endif /* QUERY_HANDLER_H */
