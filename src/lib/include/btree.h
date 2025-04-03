#ifndef BTREE_H
#define BTREE_H

#include <stddef.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>

/**
 * @brief Btree node structure
 *
 * This represents an internal node in the B-tree.
 *
 * Since, we're optimizing for just lookups, we decided to:
 * - Fill factor = 100% (all internal nodes are full)
 * - internal nodes are contiguous in memory
 *      - store the keys in an array of integers
 *      - a pointer to the next level of the tree (the first element of the next level)
 * - The last level's child_ptr always points to the `data` array
 *
 * Since during lookups, we need all values to compare against, and only follow one
 * pointer (see CS165_FALL2024_Class13 slide 26 & 27).
 *
 *
 */
typedef struct Btree {
  struct Btree* child_ptr;
  int* keys;
  size_t n_keys;

  // Global bookkeeping for unique values, etc.
  size_t fanout;
  size_t n_uniques;
  size_t* first_unique_idxes;
  size_t* last_unique_idxes;
} Btree;

// TODO: refactor the above by moving global bookkeeping to a separate struct, say
// BtreeIndex below, time permitting. they don't change per level node
// typedef struct BtreeIndex {
//   Btree* root;
//   size_t fanout;
//   size_t n_uniques;
//   size_t* first_unique_idxes;
//   size_t* last_unique_idxes;
// };

Btree* init_btree(int* data, size_t n_elts, size_t fanout);

/**
 * @brief Lookup a key in the B-tree.
 *
 * @param key The key to search for.
 * @param tree The root of the B-tree.
 * @param is_left indicator for whether the caller is looking for first or last occurence
 * @return an index `i` in the `data` array such that
 *      `data[i - 1] < data[i] <= key` for i > 0 else 0,      if `is_left` is true (1)
 * OR
 *      `data[i] >= key > data[i + 1]` for i < n_elts,        if `is_left` is false (0)
 */
size_t lookup(int key, Btree* tree, int is_left);

void print_tree(Btree* tree);

/**
 * @brief Free all memory allocated for the B-tree; not the data array.
 *
 * @param tree The root of the B-tree.
 */
void free_btree(Btree* tree);

void test_btree(void);

#endif
