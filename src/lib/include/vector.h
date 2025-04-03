#ifndef VECTOR_H
#define VECTOR_H

#include <stddef.h>

typedef struct {
  void** data;                // Array of pointers to elements
  size_t size;                // Current number of elements
  size_t capacity;            // Total space allocated
  void (*destructor)(void*);  // Function to clean up elements
} Vector;

// Core operations
Vector* vector_create(void (*destructor)(void*));
void vector_destroy(Vector* vec);
void vector_push_back(Vector* vec, void* element);
void* vector_get(const Vector* vec, size_t index);
void vector_set(Vector* vec, size_t index, void* element);
size_t vector_size(const Vector* vec);

#endif
