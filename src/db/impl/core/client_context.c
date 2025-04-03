#include "client_context.h"

#include <errno.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define INITIAL_CHANDLE_SLOTS 1000
#define GROWTH_FACTOR 2

// TODO: Extra: to support multiple clients, consider not using a global client
// context. Using global now for simplicity.
ClientContext *g_client_context = NULL;

static bool is_valid_handle_name(const char *name) {
  return name != NULL && strlen(name) < MAX_SIZE_NAME;
}

void init_client_context(void) {
  if (g_client_context != NULL) {
    return;
  }

  g_client_context = (ClientContext *)calloc(1, sizeof(ClientContext));
  if (!g_client_context) {
    log_err("init_client_context: failed to allocate memory for client context\n");
    exit(1);
  }

  g_client_context->chandle_table =
      (Column *)calloc(INITIAL_CHANDLE_SLOTS, sizeof(Column));
  if (!g_client_context->chandle_table) {
    free(g_client_context);
    g_client_context = NULL;
    log_err("init_client_context: failed to allocate memory for chandle table\n");
    return;
  }

  g_client_context->chandles_in_use = 0;
  g_client_context->chandle_slots = INITIAL_CHANDLE_SLOTS;
  g_client_context->is_batch_queries_on = 0;
  g_client_context->is_single_core = 0;
  log_info("Client context initialized\n");
  return;
}

void free_client_context(void) {
  if (!g_client_context) return;

  if (g_client_context->chandle_table) {
    for (int i = 0; i < g_client_context->chandles_in_use; i++) {
      Column *col = &g_client_context->chandle_table[i];
      if (col) {
        // we ignore closing col->disk_fd and munmap(col->data, col->mmap_size) since
        // such columns should be managed by the catalog manager. this is variable pool.
        cs165_log(stdout, "free_client_context: Freeing column %s\n", col->name);
        free(col->data);
        memset(col, 0, sizeof(Column));  // Clear sensitive data
      }
    }
    free(g_client_context->chandle_table);
  }

  free(g_client_context);
  g_client_context = NULL;
}

int create_new_handle(const char *name, Column **out_column) {
  if (!g_client_context) {
    log_err("create_new_handle: client context is not initialized\n");
    return -1;
  }

  if (!is_valid_handle_name(name) || !out_column) {
    log_err("create_new_handle: invalid arguments\n");
    return -1;
  }

  // Check for duplicate names.
  //    TODO: removing this until we have an O(1) lookup. Currently, we don't expect
  //    many handles to be created: > 100, >1000?
  //   Also, what number of handles would be worth the overhead of a hash table?m
  //   if (get_handle(name) != NULL) {
  //     log_err("create_new_handle: handle with name %s already exists\n", name);
  //     return -1;
  //   }

  // Check if resize needed
  if (g_client_context->chandles_in_use >= g_client_context->chandle_slots) {
    size_t new_size = g_client_context->chandle_slots * GROWTH_FACTOR;

    // TODO: Fix the issue that the actual columns gets freed and not copied back into
    // the new memory. To reproduce the issue, use a small INITIAL_CHANDLE_SLOTS value
    // Currently, using larger INITIAL_CHANDLE_SLOTS value to avoid the issue.
    Column *new_table =
        (Column *)realloc(g_client_context->chandle_table, new_size * sizeof(Column));
    if (!new_table) {
      log_err("create_new_handle: failed to resize chandle table\n");
      return -1;
    }

    // Zero initialize new memory
    memset(new_table + g_client_context->chandle_slots, 0,
           (new_size - g_client_context->chandle_slots) * sizeof(Column));

    g_client_context->chandle_table = new_table;
    g_client_context->chandle_slots = new_size;
  }

  // Initialize new handle
  Column *new_col = &g_client_context->chandle_table[g_client_context->chandles_in_use];
  memset(new_col, 0, sizeof(Column));
  snprintf(new_col->name, MAX_SIZE_NAME, "handle_%s", name);

  *out_column = new_col;
  g_client_context->chandles_in_use++;
  log_info("create_new_handle: created new handle %s at i=%d\n", name,
           g_client_context->chandles_in_use - 1);
  return 0;
}

Column *get_handle(const char *name_) {
  if (!g_client_context || !name_) {
    log_err("get_handle: invalid arguments\n");
    return NULL;
  }
  // adjust the name to include the handle_ prefix
  char name[MAX_SIZE_NAME];
  snprintf(name, MAX_SIZE_NAME, "handle_%s", name_);
  // Search from most recent to oldest
  cs165_log(stdout, "get_handle: searching for handle %s\n", name);
  for (int i = g_client_context->chandles_in_use; i > 0; i--) {
    cs165_log(stdout, "handle at i=%d: %s\n", i - 1,
              g_client_context->chandle_table[i - 1].name);
    Column *col = &g_client_context->chandle_table[i - 1];
    if (col && strcmp(col->name, name) == 0) {
      return col;
    }
  }
  return NULL;
}
