#include "catalog_manager.h"

#include <dirent.h>
#include <errno.h>
#include <fcntl.h>
#include <string.h>    // For strncpy
#include <sys/stat.h>  // For mkdir
#include <sys/types.h>
#include <unistd.h>

#include "btree.h"
#include "common.h"
#include "optimizer.h"
#include "utils.h"

void print_column(Column *col);

bool is_valid_index_type(int value) {
  switch (value) {
    case BTREE_CLUSTERED:
    case BTREE_UNCLUSTERED:
    case SORTED_CLUSTERED:
    case SORTED_UNCLUSTERED:
    case NONE:
      return true;
    default:
      return false;
  }
}

Status deserialize_column(Column *col, Table *table, FILE *meta_file) {
  col->data_type = INT;  // Default to INT for the scope of this project
  size_t num_elements;
  long min_value, max_value, sum;
  int idx_type;

  // Load column metadata
  if (fscanf(meta_file,
             "COLUMN_NAME=%s\nNUM_ELEMENTS=%zu\nMIN_VALUE=%ld\nMAX_VALUE=%ld\n"
             "SUM=%ld\nINDEX_TYPE=%d\n",
             col->name, &num_elements, &min_value, &max_value, &sum, &idx_type) != 6) {
    log_err("init_db_from_disk: Failed to read column metadata for table %s\n",
            table->name);
    return (Status){ERROR, "Failed to read column metadata"};
  }

  // Set column metadata
  col->num_elements = num_elements;
  col->min_value = min_value;
  col->max_value = max_value;
  col->sum = sum;
  col->is_dirty = 0;

  // Construct the path to the column's data file
  char col_path[MAX_PATH_LEN];
  snprintf(col_path, MAX_PATH_LEN, "disk/%s.%s.%s.bin", current_db->name, table->name,
           col->name);

  // Open the column data file
  col->disk_fd = open(col_path, O_RDWR);
  if (col->disk_fd < 0) {
    log_err("Failed to open column data file %s\n", col_path);
    return (Status){ERROR, "Failed to open column data file"};
  }

  // mmap the column data
  col->mmap_size = col->num_elements * sizeof(int);
  col->data = (int *)mmap(NULL, col->mmap_size, PROT_READ | PROT_WRITE, MAP_SHARED,
                          col->disk_fd, 0);
  if (col->data == MAP_FAILED) {
    log_err("Error mmapping column data");
    close(col->disk_fd);
    return (Status){ERROR, "Failed to mmap column data"};
  }

  // Handle index creation, if necessary
  if (!is_valid_index_type(idx_type)) {
    log_err("init_db_from_disk: Invalid index type %d for column %s\n", idx_type,
            col->name);
    return (Status){ERROR, "Invalid index type"};
  }
  if (idx_type != NONE) {
    col->index = (ColumnIndex *)malloc(sizeof(ColumnIndex));
    col->index->idx_type = idx_type;
    create_idx_on(col, NULL);
  } else {
    col->index = NULL;
    col->root = NULL;
  }

  log_info("Loaded in %s.%s.%s with %zu elements\n", current_db->name, table->name,
           col->name, col->num_elements);
  return (Status){OK, NULL};
}

Status init_db_from_disk(void) {
  cs165_log(stdout, "Initializing database from disk\n");

  // Open the storage directory
  DIR *dir = opendir(STORAGE_PATH);
  if (dir == NULL) {
    log_err("init_db_from_disk: Cannot open disk directory\n");
    return (Status){ERROR, "Cannot open disk directory"};
  }

  struct dirent *entry;
  struct stat entry_stat;
  char db_meta_path[MAX_PATH_LEN];

  // Find the first valid database directory
  while ((entry = readdir(dir)) != NULL) {
    snprintf(db_meta_path, MAX_PATH_LEN, "%s/%s", STORAGE_PATH, entry->d_name);
    if (stat(db_meta_path, &entry_stat) == 0 && S_ISDIR(entry_stat.st_mode) &&
        strcmp(entry->d_name, ".") != 0 && strcmp(entry->d_name, "..") != 0) {
      // Check for the metadata file in this directory
      snprintf(db_meta_path, MAX_PATH_LEN, "%s/%s.meta", STORAGE_PATH, entry->d_name);
      cs165_log(stdout, "Checking for %s database's metadata\n", entry->d_name);

      FILE *meta_file = fopen(db_meta_path, "r");
      if (meta_file) {
        cs165_log(stdout, "Found metadata file for database %s\n", entry->d_name);

        // Allocate and set the current_db object
        current_db = (Db *)malloc(sizeof(Db));
        strncpy(current_db->name, entry->d_name, MAX_SIZE_NAME);
        if (!current_db) {
          log_err("init_db_from_disk: Failed to allocate memory for current_db\n");
          fclose(meta_file);
          closedir(dir);
          return (Status){ERROR, "Failed to allocate memory for current_db"};
        }

        // Initialize variables and skip the DB_NAME line
        char temp_buf[MAX_SIZE_NAME];
        fscanf(meta_file, "DB_NAME=%s\n", temp_buf);  // Skip DB_NAME

        // Read database metadata
        size_t tables_size, tables_capacity;
        if (fscanf(meta_file, "TABLES_SIZE=%zu\nTABLES_CAPACITY=%zu\n", &tables_size,
                   &tables_capacity) != 2) {
          log_err("init_db_from_disk: Failed to read database metadata for %s\n",
                  current_db->name);
          free(current_db);
          fclose(meta_file);
          closedir(dir);
          return (Status){ERROR, "Failed to read database metadata"};
        }

        // Validate metadata values
        if (tables_size == 0 || tables_capacity == 0 || tables_size > tables_capacity) {
          log_err(
              "init_db_from_disk: Invalid tables_size or tables_capacity in metadata\n");
          free(current_db);
          fclose(meta_file);
          closedir(dir);
          return (Status){ERROR, "Invalid metadata values"};
        }

        current_db->tables_size = tables_size;
        current_db->tables_capacity = tables_capacity;

        // Allocate memory for tables
        current_db->tables = (Table *)malloc(current_db->tables_capacity * sizeof(Table));
        if (!current_db->tables) {
          log_err("init_db_from_disk: Failed to allocate memory for tables\n");
          free(current_db);
          fclose(meta_file);
          closedir(dir);
          return (Status){ERROR, "Failed to allocate memory for tables"};
        }

        // Read and initialize tables and columns
        for (size_t i = 0; i < current_db->tables_size; i++) {
          Table *table = &current_db->tables[i];
          size_t col_capacity, num_cols;
          if (fscanf(meta_file, "TABLE_NAME=%s\nCOL_CAPACITY=%zu\nNUM_COLS=%zu\n",
                     table->name, &col_capacity, &num_cols) != 3) {
            log_err("init_db_from_disk: Failed to read table metadata\n");
            free(current_db->tables);
            free(current_db);
            fclose(meta_file);
            closedir(dir);
            return (Status){ERROR, "Failed to read table metadata"};
          }

          // Validate column capacity and number of columns
          if (col_capacity == 0 || num_cols > col_capacity) {
            log_err("init_db_from_disk: Invalid column metadata for table %s\n",
                    table->name);
            free(current_db->tables);
            free(current_db);
            fclose(meta_file);
            closedir(dir);
            return (Status){ERROR, "Invalid column metadata"};
          }

          table->col_capacity = col_capacity;
          table->num_cols = num_cols;

          // Allocate memory for columns
          table->columns = (Column *)malloc(table->col_capacity * sizeof(Column));
          if (!table->columns) {
            log_err("init_db_from_disk: Failed to allocate memory for columns\n");
            free(current_db->tables);
            free(current_db);
            fclose(meta_file);
            closedir(dir);
            return (Status){ERROR, "Failed to allocate memory for columns"};
          }

          Column *primary_col = NULL;  // Primary column for indexing, this is the first
                                       // column with clustered index
          // Read column metadata and remap each column's data file
          for (size_t j = 0; j < table->num_cols; j++) {
            Column *col = &table->columns[j];
            if (deserialize_column(col, table, meta_file).code != OK) {
              free(table->columns);
              free(current_db->tables);
              free(current_db);
              fclose(meta_file);
              closedir(dir);
              return (Status){ERROR, "Failed to load column metadata"};
            }
            IndexType idx_type = col->index ? col->index->idx_type : NONE;
            if (!primary_col &&
                (idx_type == SORTED_CLUSTERED || idx_type == BTREE_CLUSTERED)) {
              primary_col = col;
            }
            print_column(col);
          }
          //   if (primary_col) {
          //     cluster_idx_on(table, primary_col, NULL);
          //   }
        }

        fclose(meta_file);
        closedir(dir);
        log_info("Database %s successfully loaded from disk\n", current_db->name);
        return (Status){OK, "Database loaded from disk"};
      } else {
        log_err("init_db_from_disk: Failed to open metadata file for %s\n",
                entry->d_name);
      }
    }
  }

  closedir(dir);
  cs165_log(stdout, "No valid database found on disk\n");
  return (Status){ERROR, "No valid database found on disk"};
}

/**
 * @brief Get the column from catalog object
 *
 * @param db_tbl_col_name e.g."db1.tbl1.col1"
 * @return Column*
 */
Column *get_column_from_catalog(const char *db_tbl_col_name) {
  cs165_log(stdout, "catalog_manager: Get column %s from catalog\n", db_tbl_col_name);
  // Ensure the current database is set
  if (current_db == NULL) {
    log_err("get_column_from_catalog: No database loaded");
    return NULL;
  }

  // Split the db_tbl_col_name into its components
  char db_name[MAX_SIZE_NAME];
  char table_name[MAX_SIZE_NAME];
  char column_name[MAX_SIZE_NAME];
  if (sscanf(db_tbl_col_name, "%[^.].%[^.].%s", db_name, table_name, column_name) != 3) {
    log_err("get_column_from_catalog: Invalid db_tbl_col_name format. Got %s\n",
            db_tbl_col_name);
    return NULL;
  }

  // Search for the column within the current database
  for (size_t i = 0; i < current_db->tables_size; i++) {
    if (strcmp(current_db->tables[i].name, table_name) == 0) {
      for (size_t j = 0; j < current_db->tables[i].num_cols; j++) {
        if (strcmp(current_db->tables[i].columns[j].name, column_name) == 0) {
          log_info("get_column_from_catalog: Column %s found\n", column_name);
          return &current_db->tables[i].columns[j];
        }
      }
    }
  }
  return NULL;
}

Table *get_table_from_catalog(const char *table_name) {
  // Ensure the current database is set
  if (current_db == NULL) {
    log_err("get_table_from_catalog: No database loaded");
    return NULL;
  }

  // Search for the table within the current database
  for (size_t i = 0; i < current_db->tables_size; i++) {
    if (strcmp(current_db->tables[i].name, table_name) == 0) {
      return &current_db->tables[i];
    }
  }

  log_err("get_table_from_catalog: Table not found");
  return NULL;
}

Status shutdown_catalog_manager(void) {
  cs165_log(stdout, "Shutting down catalog manager\n");
  if (!current_db) {
    cs165_log(stdout, "No active database to shutdown\n");
    return (Status){ERROR, "No active database to shutdown"};
  }

  // Open metadata file for writing
  char meta_path[MAX_PATH_LEN];
  snprintf(meta_path, MAX_PATH_LEN, "%s/%s.meta", STORAGE_PATH, current_db->name);
  FILE *meta_file = fopen(meta_path, "w");
  if (!meta_file) {
    log_err("Failed to open metadata file for writing: %s\n", meta_path);
    return (Status){ERROR, "Failed to write metadata"};
  }

  // Write database metadata (name, tables_size, tables_capacity)
  fprintf(meta_file, "DB_NAME=%s\nTABLES_SIZE=%zu\nTABLES_CAPACITY=%zu\n",
          current_db->name, current_db->tables_size, current_db->tables_capacity);

  // Iterate over each table in the database
  for (size_t i = 0; i < current_db->tables_size; i++) {
    Table *table = &current_db->tables[i];
    fprintf(meta_file, "TABLE_NAME=%s\nCOL_CAPACITY=%zu\nNUM_COLS=%zu\n", table->name,
            table->col_capacity, table->num_cols);

    // Iterate over each column in the table
    for (size_t j = 0; j < table->num_cols; j++) {
      Column *col = &table->columns[j];
      int idx_type = col->index ? col->index->idx_type : NONE;

      cs165_log(stdout, "shutting down column %s\n", col->name);
      print_column(col);

      fprintf(meta_file,
              "COLUMN_NAME=%s\nNUM_ELEMENTS=%zu\nMIN_VALUE=%ld\nMAX_VALUE=%ld\nSUM=%ld\n"
              "INDEX_TYPE=%d\n",
              col->name, col->num_elements, col->min_value, col->max_value, col->sum,
              idx_type);

      if (idx_type != NONE) {
        if (col->index->idx_type == BTREE_CLUSTERED ||
            col->index->idx_type == BTREE_UNCLUSTERED) {
          free_btree(col->root);
        }
        free(col->index->sorted_data);
        free(col->index->positions);
        free(col->index);
      }

      if (col->is_dirty) {
        // Only truncate and sync the actual data size
        size_t actual_size = col->num_elements * sizeof(int);
        if (ftruncate(col->disk_fd, actual_size) == -1) {
          log_err("Error truncating column data file");
        }
        if (msync(col->data, actual_size, MS_SYNC) == -1) {
          log_err("Error syncing memory to disk");
        }
      }
      // unmap the column data and close the file descriptor
      if (col->data) {
        cs165_log(stdout, "num_elements: %zu\n", col->num_elements);
        if (munmap(col->data, col->mmap_size) == -1) {
          log_err("Error unmapping memory");
        }
        if (col->disk_fd > 0) {
          cs165_log(stdout, "closing fd: %d\n", col->disk_fd);
          close(col->disk_fd);
        }
      }
    }
    free(table->columns);
  }

  free(current_db->tables);
  fclose(meta_file);
  free(current_db);

  cs165_log(stdout, "Metadata written and catalog manager shut down.\n");
  return (Status){OK, "Catalog manager: database closed and metadata saved"};
}

void print_column(Column *col) {
#ifdef LOG
  if (col == NULL) {
    log_err("vizualize_column: column is NULL\n");
    return;
  }

  if (col->data == NULL) {
    log_err("vizualize_column: column data is NULL\n");
    return;
  }
  log_info("\n\n\nColumn: %s\n", col->name);
  cs165_log(stdout, "Number of elements: %zu\n", col->num_elements);

  if (col->num_elements == 0) {
    log_info("No data to display\n");
    return;
  }

  if (col->num_elements <= 1024) {
    //   show tree structure
    if (col->root) {
      log_info("BTree structure:\n----------------\n");
      print_tree(col->root);
      log_info("----------------\n");
    }

    if (col->index && col->index->idx_type != NONE) {
      log_info("Sorted layer: \n================\n");
      for (size_t i = 0; i < col->num_elements; i++) {
        printf("(val: %d, pos: %d) ", col->index->sorted_data[i],
               col->index->positions[i]);
      }
      printf("\n================\n");
    }

    log_info("Disk data:\n**********\n");
    for (size_t i = 0; i < col->num_elements; i++) {
      printf("%d ", ((int *)col->data)[i]);
    }
    printf("\n**********\n");

  } else {
    log_info("Data too large to display\n");
  }

  printf("\nStats Metadata:\n");
  cs165_log(stdout, "Min value: %d\n", col->min_value);
  cs165_log(stdout, "Max value: %d\n", col->max_value);
  cs165_log(stdout, "Sum: %ld\n", col->sum);

  printf("\n\n\n");
#else
  (void)col;
#endif
}
