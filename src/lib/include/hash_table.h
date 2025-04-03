#ifndef HASH_TABLE_H
#define HASH_TABLE_H

typedef int keyType;
typedef int valType;

// Node structure for the linked list of values
typedef struct node {
  valType value;
  struct node* next;
} node;

// Entry structure for each key-value pair
typedef struct entry {
  keyType key;
  node* values;
  struct entry* next;
} entry;

// Hash table structure
typedef struct hashtable {
  entry** buckets;
  int size;
  int num_buckets;
} hashtable;

int allocate(hashtable** ht, int size);
int put(hashtable* ht, keyType key, valType value);
int get(hashtable* ht, keyType key, valType* values, int num_values, int* num_results);
int erase(hashtable* ht, keyType key);
int deallocate(hashtable* ht);

int test_hashmap(void);

#endif
