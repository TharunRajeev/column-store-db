#ifndef PARSE_H__
#define PARSE_H__
#include "client_context.h"
#include "common.h"
#include "operators.h"

DbOperator *parse_command(char *query_command, message *send_message, int client,
                          ClientContext *context);

#endif
