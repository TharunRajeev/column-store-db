#ifndef DB_H
#define DB_H

#include <stdlib.h>

#include "btree.h"
#include "common.h"

/**
 * @brief ColumnIndex is the sorted copy of the base data in a column.
 * We keep the data and their positions as separate arrays since the application mostly
 * needs one or the other. For example, for select, we need to binary search the data,
 * but fetch would be interested in the positions of qualifying data. So no need to pull
 * all data and positions in memory, if only working with one.
 *
 * - `sorted_data`: the sorted data array
 * - `positions`: the positions of the data in the original array
 * - `idx_type`: the type of index (see `IndexType` enum)
 * The number of elements in the `sorted_data` and `positions` arrays must be the same as
 * column's `Column->num_elements`. So storing it would be redundant (maybe helpful tho)
 */
typedef struct ColumnIndex {
  int *sorted_data;
  int *positions;
  IndexType idx_type;
} ColumnIndex;

typedef struct Column {
  char name[MAX_SIZE_NAME];
  DataType data_type;
  Btree *root;
  ColumnIndex *index;
  void *data;
  size_t mmap_size;  // can be derived from mmap_size (clean up later), also a result
                     // column doesn't need to have this; what'd this mean for `insert`?
  int disk_fd;
  int is_dirty;  // a flag to indicate if the column has been modified
  //   void *index;
  size_t num_elements;
  // Stat metrics
  long min_value;
  long max_value;
  int64_t sum;
} Column;

/**
 * table
 * Defines a table structure, which is composed of multiple columns.
 * We do not require you to dynamically manage the size of your tables,
 * although you are free to append to the struct if you would like to (i.e.,
 * include a size_t table_size).
 * name, the name associated with the table. table names must be unique
 *     within a database, but tables from different databases can have the same
 *     name.
 * - col_capacity, the maximum number of columns that can be held in the table.
 * - columns this is the pointer to an array of columns contained in the table.
 * - num_cols, the number of columns currently held in the table.
 **/
typedef struct Table {
  char name[MAX_SIZE_NAME];
  Column *columns;
  size_t col_capacity;
  size_t num_cols;
} Table;

/**
 * db
 * Defines a database structure, which is composed of multiple tables.
 * - name: the name of the associated database.
 * - tables: the pointer to the array of tables contained in the db.
 * - tables_size: the size of the array holding table objects
 * - tables_capacity: the amount of pointers that can be held in the currently
 *allocated memory slot
 **/

typedef struct Db {
  char name[MAX_SIZE_NAME];
  Table *tables;
  size_t tables_size;
  size_t tables_capacity;
} Db;

extern Db *current_db;

/*
 * Use this command to see if databases that were persisted start up properly. If
 * files don't load as expected, this can return an error.
 */
Status db_startup(void);

void db_shutdown(void);

#endif  // DB_H
