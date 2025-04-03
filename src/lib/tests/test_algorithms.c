#include <assert.h>
#include <stdio.h>
#include <stdlib.h>

#include "algorithms.h"

void test_sort(void) {
  // Test 1: Normal case with unsorted positive integers
  {
    printf("test for unsorted positive integers...");
    int data[] = {50, 20, 10, 40, 30};
    size_t n_elements = sizeof(data) / sizeof(data[0]);
    int original_pos[n_elements];
    int expected_data[] = {10, 20, 30, 40, 50};
    int expected_pos[] = {2, 1, 4, 3, 0};

    int result = sort(data, n_elements, original_pos);
    assert(result == 0);

    for (size_t i = 0; i < n_elements; i++) {
      assert(data[i] == expected_data[i]);
      assert(original_pos[i] == expected_pos[i]);
    }
    printf("✅\n");
  }

  // Test 2: Array with negative integers
  {
    printf("test for negative integers...");
    int data[] = {-10, -50, 20, 0, -30};
    size_t n_elements = sizeof(data) / sizeof(data[0]);
    int original_pos[n_elements];
    int expected_data[] = {-50, -30, -10, 0, 20};
    int expected_pos[] = {1, 4, 0, 3, 2};

    int result = sort(data, n_elements, original_pos);
    assert(result == 0);

    for (size_t i = 0; i < n_elements; i++) {
      assert(data[i] == expected_data[i]);
      assert(original_pos[i] == expected_pos[i]);
    }
    printf("✅\n");
  }

  // Test 3: Array with duplicates
  {
    printf("test for duplicates...");
    int data[] = {10, 20, 10, 40, 20};
    size_t n_elements = sizeof(data) / sizeof(data[0]);
    int original_pos[n_elements];
    int expected_data[] = {10, 10, 20, 20, 40};
    int expected_pos[] = {0, 2, 1, 4, 3};

    int result = sort(data, n_elements, original_pos);
    assert(result == 0);

    for (size_t i = 0; i < n_elements; i++) {
      assert(data[i] == expected_data[i]);
      assert(original_pos[i] == expected_pos[i]);
    }
    printf("✅\n");
  }

  // Test 4: Single-element array
  {
    printf("test for single-element array...");
    int data[] = {42};
    size_t n_elements = 1;
    int original_pos[n_elements];
    int expected_data[] = {42};
    int expected_pos[] = {0};

    int result = sort(data, n_elements, original_pos);
    assert(result == 0);

    for (size_t i = 0; i < n_elements; i++) {
      assert(data[i] == expected_data[i]);
      assert(original_pos[i] == expected_pos[i]);
    }
    printf("✅\n");
  }

  // Test 5: Empty array
  {
    printf("test for empty array...");
    int* data = NULL;
    size_t n_elements = 0;
    int* original_pos = NULL;

    int result = sort(data, n_elements, original_pos);
    assert(result == -1);  // Should fail gracefully
    printf("✅\n");
  }

  // Test 6: NULL data pointer
  {
    printf("test for NULL data pointer...");
    int* data = NULL;
    size_t n_elements = 5;
    int original_pos[n_elements];

    int result = sort(data, n_elements, original_pos);
    assert(result == -1);  // Should fail gracefully
    printf("✅\n");
  }

  // Test 7: NULL original_pos pointer
  {
    printf("test for NULL original_pos pointer...");
    int data[] = {10, 20, 30};
    size_t n_elements = sizeof(data) / sizeof(data[0]);
    int* original_pos = NULL;

    int result = sort(data, n_elements, original_pos);
    assert(result == -1);  // Should fail gracefully
    printf("✅\n");
  }

  // Test 8: Large dataset
  {
    printf("test for large dataset...");
    size_t n_elements = 100000;
    int* data = (int*)malloc(n_elements * sizeof(int));
    int* original_pos = (int*)malloc(n_elements * sizeof(int));

    for (size_t i = 0; i < n_elements; i++) {
      data[i] = rand() % 1000000;  // Random values
    }

    int result = sort(data, n_elements, original_pos);
    assert(result == 0);

    for (size_t i = 1; i < n_elements; i++) {
      assert(data[i] >= data[i - 1]);  // Ensure sorted
    }

    free(data);
    free(original_pos);
    printf("✅\n");
  }
}
