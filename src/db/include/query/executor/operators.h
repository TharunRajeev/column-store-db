#ifndef OPERATORS_H
#define OPERATORS_H

#include "client_context.h"
#include "common.h"

/*
 * necessary fields for creation
 * "create_type" indicates what kind of object you are creating.
 * For example, if create_type == _DB, the operator should create a db named <<name>>
 * if create_type = _TABLE, the operator should create a table named <<name>> with
 * <<col_count>> columns within db <<db>> if create_type = = _COLUMN, the operator
 * should create a column named
 * <<name>> within table <<table>>
 */
typedef struct CreateOperator {
  CreateType create_type;
  char name[MAX_SIZE_NAME];
  Db *db;
  Table *table;
  int col_count;
} CreateOperator;

typedef struct CreateIndexOperator {
  Column *col;
  IndexType idx_type;
} CreateIndexOperator;

/*
 * necessary fields for insertion
 */
typedef struct InsertOperator {
  Table *table;
  int *values;
} InsertOperator;
/*
 * necessary fields for insertion
 */
typedef struct LoadOperator {
  char *file_name;
} LoadOperator;

/**
 * comparator
 * A comparator defines a comparison operation over a column.
 **/
typedef struct Comparator {
  Column *col;      // the column to compare against.
  int *ref_posns;   // original positions of the values in the column.
  long int p_low;   // used in equality and ranges.
  long int p_high;  // used in range compares.
  ComparatorType type1;
  ComparatorType type2;
  int on_sorted_data;
} Comparator;

typedef struct SelectOperator {
  char *res_handle;
  Comparator *comparator;
} SelectOperator;

typedef struct FetchOperator {
  char *fetch_handle;
  char *select_handle;
  Column *col;
} FetchOperator;

typedef struct AggregateOperator {
  Column *col;
  char *res_handle;
} AggregateOperator;

typedef struct ArithmeticOperator {
  Column *col1;
  Column *col2;
  char *res_handle;
} ArithmeticOperator;

typedef struct PrintOperator {
  Column **columns;
  size_t num_columns;
} PrintOperator;

typedef struct JoinOperator {
  Column *posn1;
  Column *posn2;
  Column *vals1;
  Column *vals2;
  char *res_handle1;
  char *res_handle2;
  JoinType join_type;
} JoinOperator;

/*
 * union type holding the fields of any operator
 */
typedef union OperatorFields {
  CreateOperator create_operator;
  CreateIndexOperator create_index_operator;
  InsertOperator insert_operator;
  LoadOperator load_operator;
  SelectOperator select_operator;
  FetchOperator fetch_operator;
  PrintOperator print_operator;
  AggregateOperator aggregate_operator;
  ArithmeticOperator arithmetic_operator;
  JoinOperator join_operator;
} OperatorFields;
/*
 * DbOperator holds the following fields:
 * type: the type of operator to perform (i.e. insert, select, ...)
 * operator fields: the fields of the operator in question
 * client_fd: the file descriptor of the client that this operator will return to
 * context: the context of the operator in question. This context holds the local
 * results of the client in question.
 */
typedef struct DbOperator {
  OperatorType type;
  OperatorFields operator_fields;
  int client_fd;
  ClientContext *context;
} DbOperator;

void db_operator_free(DbOperator *query);

#endif
