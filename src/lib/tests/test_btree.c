#include <assert.h>
#include <stdio.h>
#include <stdlib.h>

#include "btree.h"
#include "test_helpers.h"
#include "utils.h"

void test_btree(void) {
  test_title("\nB-tree tests: \n");
  {
    /* The Simple tree version: n_elements = 8, fanout = 2
            B-tree structure:
                    [child_ptr][10,50]
                            |
                [child_ptr][10,30,50,70]
                     |
        data        [10 20 30 40 50 60 70 80]
        index       [ 0  1  2  3  4  5  6  7]
     */
    test_sub_title("\nTest 1: Lookup in a B-tree with uniqe numbers; fanout 2\n");
    int data[] = {10, 20, 30, 40, 50, 60, 70, 80};
    size_t data_size = sizeof(data) / sizeof(data[0]);
    int fanout = 2;

    // Initialize B-tree
    Btree* tree = init_btree(data, data_size, fanout);
    printf("\nB-tree structure:\n");
    print_tree(tree);

    // Test exact matches (elements at fanout boundaries)
    for (size_t i = 0; i < data_size; i++) {
      size_t pos_r = lookup(data[i], tree, 0);
      size_t pos_l = lookup(data[i], tree, 1);

      // since the data is unique, the left and right lookups should be the same
      assert_nice(pos_l, i, ", ");
      assert_nice(pos_r, i, ", ");
      printf("\n");
    }

    {
      test_title("\nTest 4: Lookup in a B-tree with duplicate numbers; fanout 2\n");
      int data[] = {2, 2, 2, 5, 5, 6, 7, 7};
      size_t data_size = sizeof(data) / sizeof(data[0]);
      int fanout = 2;
      printf("Data: ");
      for (size_t i = 0; i < data_size; i++) {
        printf("%d ", data[i]);
      }
      printf("\n");
      Btree* tree = init_btree(data, data_size, fanout);
      printf("\nB-tree structure:\n");
      print_tree(tree);

      {
        printf("checking bounds: ");
        size_t pos_l = lookup(2, tree, 1);
        size_t pos_r = lookup(2, tree, 0);
        assert_nice(pos_l, 0, ", ");
        assert_nice(pos_r, 2, ", ");
      }
      {
        size_t pos_l = lookup(7, tree, 1);
        size_t pos_r = lookup(7, tree, 0);
        assert_nice(pos_l, 6, ", ");
        assert_nice(pos_r, 7, "\n");
      }
      {
        printf("checking middle duplicates: ");
        size_t pos_l = lookup(5, tree, 1);
        size_t pos_r = lookup(5, tree, 0);
        assert_nice(pos_l, 3, ", ");
        assert_nice(pos_r, 4, "\n");
      }
      {
        printf("checking middle unique: ");
        size_t pos_l = lookup(6, tree, 1);
        size_t pos_r = lookup(6, tree, 0);
        assert_nice(pos_l, 5, ", ");
        assert_nice(pos_r, 5, "\n");
      }
    }
  }
}
