#ifndef CLIENT_CONTEXT_H
#define CLIENT_CONTEXT_H

#include "db.h"
#include "utils.h"
#include "vector.h"

/*
 * holds the information necessary to refer to a result column
 */
typedef struct ClientContext {
  Column *chandle_table;
  int chandles_in_use;
  int chandle_slots;
  int is_batch_queries_on;
  int is_single_core;
  Vector *bselect_dbos;  // Vector of DbOperators for batched select queries
} ClientContext;

extern ClientContext *g_client_context;

void init_client_context(void);
void free_client_context(void);
int create_new_handle(const char *name, Column **out_column);
Column *get_handle(const char *name);

#endif
