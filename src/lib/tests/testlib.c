#include <stdio.h>

#include "algorithms.h"
#include "btree.h"
#include "hash_table.h"

int main(void) {
  printf("\n\ntesting sort...\n");
  test_sort();

  printf("\n\ntesting btree...\n");
  test_btree();

  printf("\n\nAll tests passed!\n");

  printf("\n\ntesting hashmap...\n");
  test_hashmap();

  return 0;
}
