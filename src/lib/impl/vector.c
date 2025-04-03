#include "../include/vector.h"

#include <stdlib.h>

#define INITIAL_CAPACITY 10
#define GROWTH_FACTOR 2

Vector* vector_create(void (*destructor)(void*)) {
  Vector* vec = malloc(sizeof(Vector));
  if (!vec) return NULL;

  // Allocate initial array of pointers
  vec->data = malloc(sizeof(void*) * INITIAL_CAPACITY);
  if (!vec->data) {
    free(vec);
    return NULL;
  }

  vec->size = 0;
  vec->capacity = INITIAL_CAPACITY;
  vec->destructor = destructor;
  return vec;
}

void vector_destroy(Vector* vec) {
  if (!vec) return;

  // Clean up each element if destructor is provided
  if (vec->destructor) {
    for (size_t i = 0; i < vec->size; i++) {
      if (vec->data[i]) {
        vec->destructor(vec->data[i]);
      }
    }
  }

  // Free the array and the vector itself
  free(vec->data);
  free(vec);
}

static int vector_grow(Vector* vec) {
  size_t new_capacity = vec->capacity * GROWTH_FACTOR;
  void** new_data = realloc(vec->data, sizeof(void*) * new_capacity);

  if (!new_data) return 0;  // Growth failed

  vec->data = new_data;
  vec->capacity = new_capacity;
  return 1;  // Success
}

void vector_push_back(Vector* vec, void* element) {
  if (!vec) return;

  // Grow if necessary
  if (vec->size == vec->capacity) {
    if (!vector_grow(vec)) return;  // Failed to grow
  }

  vec->data[vec->size++] = element;
}

void* vector_get(const Vector* vec, size_t index) {
  if (!vec || index >= vec->size) return NULL;
  return vec->data[index];
}

void vector_set(Vector* vec, size_t index, void* element) {
  if (!vec || index >= vec->size) return;

  // Clean up existing element if necessary
  if (vec->destructor && vec->data[index]) {
    vec->destructor(vec->data[index]);
  }

  vec->data[index] = element;
}

size_t vector_size(const Vector* vec) { return vec ? vec->size : 0; }
