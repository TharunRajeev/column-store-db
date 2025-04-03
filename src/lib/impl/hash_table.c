#include "hash_table.h"

#include <stdlib.h>

// TODO: Improve the hash function
static unsigned int hash(keyType key, int num_buckets) {
  return (unsigned int)key % num_buckets;
}

int allocate(hashtable** ht, int size) {
  if (ht == NULL || size <= 0) {
    return -1;
  }

  // Allocate the hash table structure
  *ht = (hashtable*)malloc(sizeof(hashtable));
  if (*ht == NULL) {
    return -1;
  }

  // Use size as number of buckets for simplicity
  (*ht)->num_buckets = size;
  (*ht)->size = 0;

  // Allocate the buckets array
  (*ht)->buckets = (entry**)calloc(size, sizeof(entry*));
  if ((*ht)->buckets == NULL) {
    free(*ht);
    *ht = NULL;
    return -1;
  }

  return 0;
}

int put(hashtable* ht, keyType key, valType value) {
  if (ht == NULL) {
    return -1;
  }

  unsigned int bucket = hash(key, ht->num_buckets);

  // Create new value node
  node* new_value = (node*)malloc(sizeof(node));
  if (new_value == NULL) {
    return -1;
  }
  new_value->value = value;
  new_value->next = NULL;

  // Find or create entry for this key
  entry* current = ht->buckets[bucket];
  while (current != NULL) {
    if (current->key == key) {
      // Add value to existing key's list
      new_value->next = current->values;
      current->values = new_value;
      return 0;
    }
    current = current->next;
  }

  // Create new entry
  entry* new_entry = (entry*)malloc(sizeof(entry));
  if (new_entry == NULL) {
    free(new_value);
    return -1;
  }

  new_entry->key = key;
  new_entry->values = new_value;
  new_entry->next = ht->buckets[bucket];
  ht->buckets[bucket] = new_entry;
  ht->size++;

  return 0;
}

int get(hashtable* ht, keyType key, valType* values, int num_values, int* num_results) {
  if (ht == NULL || values == NULL || num_results == NULL || num_values <= 0) {
    return -1;
  }

  *num_results = 0;
  unsigned int bucket = hash(key, ht->num_buckets);

  // Find entry with matching key
  entry* current = ht->buckets[bucket];
  while (current != NULL) {
    if (current->key == key) {
      // Copy values to output array
      node* val = current->values;
      while (val != NULL) {
        if (*num_results < num_values) {
          values[*num_results] = val->value;
        }
        (*num_results)++;
        val = val->next;
      }
      return 0;
    }
    current = current->next;
  }

  return 0;
}

int erase(hashtable* ht, keyType key) {
  if (ht == NULL) {
    return -1;
  }

  unsigned int bucket = hash(key, ht->num_buckets);
  entry* current = ht->buckets[bucket];
  entry* prev = NULL;

  while (current != NULL) {
    if (current->key == key) {
      // Free all value nodes
      node* val = current->values;
      while (val != NULL) {
        node* temp = val;
        val = val->next;
        free(temp);
      }

      // Remove entry from bucket list
      if (prev == NULL) {
        ht->buckets[bucket] = current->next;
      } else {
        prev->next = current->next;
      }

      free(current);
      ht->size--;
      return 0;
    }
    prev = current;
    current = current->next;
  }

  return 0;
}

int deallocate(hashtable* ht) {
  if (ht == NULL) {
    return -1;
  }

  // Free all entries and their value lists
  for (int i = 0; i < ht->num_buckets; i++) {
    entry* current = ht->buckets[i];
    while (current != NULL) {
      // Free value list
      node* val = current->values;
      while (val != NULL) {
        node* temp_val = val;
        val = val->next;
        free(temp_val);
      }

      // Free entry
      entry* temp_entry = current;
      current = current->next;
      free(temp_entry);
    }
  }

  // Free buckets array and hash table structure
  free(ht->buckets);
  free(ht);

  return 0;
}
