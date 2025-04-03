#include "btree.h"

#include <sys/types.h>

#include "algorithms.h"
#include "btree.h"
#include "utils.h"
/**
 * @brief Create a Btree level node
 *
 * @pre 🚨 The Caller must ensure that `data` array is sorted and has unique values!!
 *
 * @param data
 * @param n_uniques
 * @param stride
 * @param fanout
 * @param first_idxes
 * @param last_idxes
 * ️
 * @return Btree*
 */
Btree* create_level(int* data, size_t n_uniques, size_t stride, size_t fanout,
                    size_t* first_idxes, size_t* last_idxes) {
  size_t n_keys = stride > 0 ? (n_uniques + stride - 1) / stride : 0;
  if (n_keys == 0) return NULL;

  Btree* node = malloc(sizeof(Btree));
  node->keys = malloc(sizeof(int) * n_keys);
  node->n_keys = n_keys;
  node->child_ptr = NULL;

  // Global bookkeeping; see todo in btree.h
  node->fanout = fanout;
  node->first_unique_idxes = first_idxes;
  node->last_unique_idxes = last_idxes;
  node->n_uniques = n_uniques;

  // Take every stride-th element
  size_t key_idx = 0;
  for (size_t i = 0; i < n_uniques && key_idx < n_keys; i += stride) {
    node->keys[key_idx] = data[i];
    key_idx++;
  }

  return node;
}

Btree* init_btree(int* data, size_t n_elts, size_t fanout) {
  if (!data || n_elts == 0 || fanout < 2) return NULL;
  /*
  Some book keeping first: keep elements in `data` unique and store their first
  and last occurrences (indices) of unique vals in the `data` array
  If we have sample input: data[] = {2, 2, 2, 5, 5, 1, 7, 7, 7, 8}; n_elts = 10;
                                i = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9]
   We want:
        - unique_sorted = {2, 5, 1, 7, 8}
        -  first_left   = {0, 3, 5, 6, 9}
        -  first_right  = {2, 4, 5, 8, 9}

  */
  size_t* first_left = malloc(sizeof(size_t) * n_elts);
  size_t* first_right = malloc(sizeof(size_t) * n_elts);
  int* unique_sorted = malloc(sizeof(int) * n_elts);

  unique_sorted[0] = data[0];
  first_left[0] = 0;
  size_t n_uniques = 0;

  for (size_t i = 1; i < n_elts; i++) {
    if (data[i] != data[i - 1]) {
      first_right[n_uniques] = i - 1;  // Close off the current unique element
      n_uniques++;                     // Move to the next unique index
      first_left[n_uniques] = i;       // Start new unique element
      unique_sorted[n_uniques] = data[i];
    }
  }
  first_right[n_uniques] = n_elts - 1;  // Close off the last unique element
  // Set `n_uniques` to the actual number of unique elements
  n_uniques++;

  // Calculate number of levels needed: this is (log_{fanout} n_elts) - 1
  //  a -1 because the last level with all values is the `data` array itself
  size_t stride = 1;
  size_t n_levels = 0;
  while (stride * fanout < n_uniques) {
    stride *= fanout;
    n_levels++;
  }

  n_levels++;  // Add the last level

  Btree** levels = malloc(sizeof(Btree*) * (n_levels));
  size_t current_stride = stride;

  // Create each level, starting from the root
  for (size_t i = 0; i < n_levels; i++) {
    levels[i] = create_level(unique_sorted, n_uniques, current_stride, fanout, first_left,
                             first_right);
    current_stride /= fanout;
  }

  // Link the levels
  for (size_t i = 0; i < n_levels - 1; i++) {
    levels[i]->child_ptr = levels[i + 1];
  }

  // Save the root and cleanup
  Btree* root = levels[0];
  free(levels);

  return root;
}

size_t lookup(int key, Btree* tree, int is_left) {
  if (!tree || tree->n_keys == 0) return 0;  // Empty tree -> would start at 0

  Btree* current = tree;
  size_t pos = 0;

  cs165_log(stdout, "lookup: looking up %d in Btree\n", key);
  // Navigate to leaf level
  while (current && current->child_ptr) {
    if (pos < current->n_keys &&
        (is_left ? key > current->keys[pos] : key >= current->keys[pos])) {
      pos = binary_search_left(current->keys + pos, current->n_keys - pos, key);
    }

    current = current->child_ptr;
    pos = pos * current->fanout;
  }

  // At leaf level, find position
  cs165_log(stdout, "lookup: at leaf level, looking for key=%d, pos=%zu\n", key, pos);
  pos = 0;
  while (pos < current->n_keys &&
         (is_left ? key > current->keys[pos] : key >= current->keys[pos])) {
    pos++;
  }
  cs165_log(stdout, "lookup: at leaf level, found key=%d, pos=%zu\n", key, pos);

  // If we're looking for leftmost (is_left true):
  // - If key exists: return first occurrence index
  // - If key doesn't exist: return index of the first element >= key
  if (is_left) {
    if (pos < current->n_keys && current->keys[pos] == key) {
      cs165_log(stdout, "Left lookup: found key=%d at pos=%zu\n", key, pos);
      log_info("returning first_unique_idxes[%zu] = %d\n", pos,
               current->first_unique_idxes[pos]);
      return current->first_unique_idxes[pos];
    }
    cs165_log(stdout, "Right lookup: found  key=%d at pos=%zu\n", key, pos);
    cs165_log(stdout, "returning first_unique_idxes[%zu] = %d\n", pos,
              pos > 0 ? current->first_unique_idxes[pos - 1] : 0);
    return pos < current->n_keys ? current->first_unique_idxes[pos] : tree->n_keys;
  }

  // If we're looking for rightmost (is_left false):
  // - If key exists: return last occurrence index
  // - If key doesn't exist: return index of last element less than key
  else {
    if (pos < current->n_keys && current->keys[pos] == key) {
      return current->last_unique_idxes[pos];
    }
    return pos > 0 ? current->last_unique_idxes[pos - 1] : 0;
  }
}

void print_tree_helper(Btree* tree) {
  if (!tree) return;
  log_info("Node:  [");
  for (size_t i = 0; i < tree->n_keys; i++) {
    log_info(" %d ", tree->keys[i]);
  }
  log_info("]\n");

  if (tree->child_ptr) {
    print_tree(tree->child_ptr);
  }
}

void print_tree(Btree* tree) {
  if (!tree) return;
  print_tree_helper(tree);
  // Print metadata
  log_info("]\n");
  log_info("First occurence idxes: [");
  for (size_t i = 0; i < tree->n_uniques; i++) {
    log_info(" %d ", tree->first_unique_idxes[i]);
  }
  log_info("]\n");
  log_info("Last occurence idxes: [");
  for (size_t i = 0; i < tree->n_uniques; i++) {
    log_info(" %d ", tree->last_unique_idxes[i]);
  }
  log_info("]\n");
}

void free_btree_nodes(Btree* tree) {
  if (!tree) return;

  if (tree->child_ptr) {
    free_btree_nodes(tree->child_ptr);
  }
  free(tree->keys);
}

void free_btree(Btree* tree) {
  if (!tree) return;
  free_btree_nodes(tree);

  free(tree->first_unique_idxes);
  free(tree->last_unique_idxes);
  free(tree);
}
