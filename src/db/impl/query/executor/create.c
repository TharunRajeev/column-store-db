#include <dirent.h>
#include <stdbool.h>
#include <string.h>
#include <sys/stat.h>  // For mkdir

#include "query_exec.h"
#include "utils.h"

// prototypes
Status create_db(const char *db_name);
Table *create_table(Db *db, const char *name, size_t num_columns, Status *status);
Column *create_column(Table *table, const char *name, bool sorted, Status *status);

/**
 * @brief Executes a create query. This can be a
 * 1. create _DB, create _TABLE, or create _COLUMN query or
 * 2. a create index query if the query type is CREATE_INDEX
 *
 * @param query
 * @param send_message
 */
void exec_create(DbOperator *query, message *send_message) {
  // TODO: Make `create_` functions take in `send_message` and set the status there.
  //   Just for consistency and cleaner code.

  if (query->type == CREATE_INDEX) {
    Column *col = query->operator_fields.create_index_operator.col;
    IndexType idx_type = query->operator_fields.create_index_operator.idx_type;

    cs165_log(stdout, "exec_create: Creating index on column %s\n", col->name);
    col->index = malloc(sizeof(ColumnIndex));
    col->index->idx_type = idx_type;

    // Set these to NULL since all create_idx queries are before data is loaded
    // The actual index is made on during `load`
    col->index->sorted_data = NULL;
    col->index->positions = NULL;
    return;
  }

  char *res_msg = NULL;
  CreateType create_type = query->operator_fields.create_operator.create_type;
  if (create_type == _DB) {
    if (create_db(query->operator_fields.create_operator.name).code == OK) {
      res_msg = "-- Database created.";
    } else {
      res_msg = "Database creation failed.";
    }
  }

  if (create_type == _TABLE) {
    Status create_status;
    create_table(query->operator_fields.create_operator.db,
                 query->operator_fields.create_operator.name,
                 query->operator_fields.create_operator.col_count, &create_status);
    if (create_status.code != OK) {
      log_err("L%d: in execute_DbOperator: %s\n", __LINE__, create_status.error_message);
      res_msg = "Table creation failed.";
    } else {
      res_msg = "-- Table created.";
    }
  }
  if (create_type == _COLUMN) {
    Status status;
    create_column(query->operator_fields.create_operator.table,
                  query->operator_fields.create_operator.name, false, &status);
    if (status.code == OK) {
      res_msg = "-- Column created.";
    } else {
      res_msg = "Column creation failed.";
    }
  }

  send_message->status = OK_DONE;
  send_message->length = strlen(res_msg);
  send_message->payload = res_msg;
}

int create_directory(const char *path) {
  struct stat st = {0};
  if (stat(path, &st) == -1) {
    if (mkdir(path, 0777) == -1) {
      log_err("create_directory: Failed to create %s directory\n", path);
      return -1;
    }
  }
  return 0;
}

Status create_db(const char *db_name) {
  cs165_log(stdout, "Creating database %s\n", db_name);
  // Remove any of databases in STORAGE_PATH, if any. Since we will only be working with
  // one database at a time.
  DIR *dir = opendir(STORAGE_PATH);
  if (dir) {
    // close this dir and rm -r STORAGE_PATH
    closedir(dir);

#ifdef __linux__
    char cmd[MAX_PATH_LEN];
    snprintf(cmd, MAX_PATH_LEN, "rm -r %s", STORAGE_PATH);
    system(cmd);
#else
    log_err(
        "create_db: Multiple databases not supported. Please delete the existing "
        "databases manually to avoid unexpected behavior\n");
    return (Status){ERROR, "Unsupported OS"};
#endif
  }

  // Check if the db name is valid
  if (strlen(db_name) > MAX_SIZE_NAME) {
    log_err("create_db: Database name is too long\n");
    return (Status){ERROR, "Database name is too long"};
  }

  // Create the base "disk" directory if it doesn't exist
  if (create_directory(STORAGE_PATH) == -1) {
    return (Status){ERROR, "Failed to create disk directory"};
  }

  // full path for the database directory
  char db_path[MAX_PATH_LEN];
  snprintf(db_path, MAX_PATH_LEN, "disk/%s", db_name);

  // db directory
  if (create_directory(db_path) == -1) {
    return (Status){ERROR, "Failed to create database directory"};
  }

  // Set up the global current_db object
  current_db = (Db *)malloc(sizeof(Db));
  if (!current_db) {
    log_err("create_db: Failed to allocate memory for current_db\n");
    return (Status){ERROR, "Failed to allocate memory for current_db"};
  }

  strncpy(current_db->name, db_name, MAX_SIZE_NAME);
  current_db->tables = NULL;
  current_db->tables_size = 0;
  current_db->tables_capacity = 0;
  log_info("Database %s created successfully\n", db_name);
  return (Status){OK, "-- Database created and current_db initialized"};
}

Table *create_table(Db *db, const char *name, size_t num_columns, Status *status) {
  cs165_log(stdout, "Creating table %s in database %s\n", name, db->name);

  // NOTE: Maybe we should check if the table already exists in the database
  if (strlen(name) >= MAX_SIZE_NAME || num_columns <= 0) {
    *status = (Status){ERROR, "Invalid table name or number of columns"};
    return NULL;
  }

  // Create the table directory
  char table_path[MAX_PATH_LEN];
  snprintf(table_path, MAX_PATH_LEN, "%s/%s/%s", STORAGE_PATH, db->name, name);
  if (create_directory(table_path) != 0) {
    log_err("create_table: Failed to create table directory\n");
    *status = (Status){ERROR, "Failed to create table directory"};
    return NULL;
  }

  // Check if we need to resize the tables array
  if (db->tables_size >= db->tables_capacity) {
    cs165_log(stdout, "Resizing tables array curr capacity: %zu\n", db->tables_capacity);
    size_t new_capacity = db->tables_capacity == 0 ? 1 : db->tables_capacity * 2;
    Table *new_tables = realloc(db->tables, new_capacity * sizeof(Table));
    if (!new_tables) {
      *status = (Status){ERROR, "Failed to resize tables array"};
      return NULL;
    }
    db->tables = new_tables;
    db->tables_capacity = new_capacity;
  }

  // Initialize the new table
  Table *new_table = &db->tables[db->tables_size];
  strncpy(new_table->name, name, MAX_SIZE_NAME);
  new_table->columns = calloc(num_columns, sizeof(Column));
  if (!new_table->columns) {
    log_err("create_table: Failed to allocate memory for requested %zu columns\n",
            num_columns);
    *status = (Status){ERROR, "Memory allocation failed for columns"};
    return NULL;
  }

  new_table->col_capacity = num_columns;
  new_table->num_cols = 0;

  db->tables_size++;
  log_info("Table %s created successfully\n", name);
  *status = (Status){OK, "-- Table created successfully"};
  return new_table;
}

Column *create_column(Table *table, const char *name, bool sorted, Status *ret_status) {
  (void)sorted;
  cs165_log(stdout, "Creating column %s in table %s\n", name, table->name);

  if (strlen(name) >= MAX_SIZE_NAME) {
    log_err("create_column: Column name is too long\n");
    *ret_status = (Status){ERROR, "Column name is too long"};
    return NULL;
  }

  // Check if we need to resize the columns array
  if (table->num_cols >= table->col_capacity) {
    log_err("create_column: Table %s is full\n", table->name);
    *ret_status = (Status){ERROR, "Table is full"};
    return NULL;
  }

  // Initialize the new column
  Column *new_column = &table->columns[table->num_cols];
  strncpy(new_column->name, name, MAX_SIZE_NAME);
  new_column->data = NULL;
  new_column->num_elements = 0;
  new_column->min_value = 0;
  new_column->max_value = 0;
  new_column->mmap_size = 0;
  new_column->disk_fd = -1;
  new_column->index = NULL;

  table->num_cols++;
  log_info("Column %s created successfully\n", name);
  *ret_status = (Status){OK, "-- Column created successfully"};
  return new_column;
}
