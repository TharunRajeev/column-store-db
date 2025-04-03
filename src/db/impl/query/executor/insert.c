#define _GNU_SOURCE

#include <errno.h>
#include <string.h>
#include <sys/mman.h>  // for mremap
#include <unistd.h>    // for sysconf

#include "query_exec.h"
#include "utils.h"

int *extend_and_update_mmap(int *mapped_addr, size_t *current_size, size_t offset,
                            const int *new_values, size_t count, int disk_fd) {
  if (mapped_addr == NULL || current_size == NULL || new_values == NULL || count == 0) {
    errno = EINVAL;
    return NULL;
  }

  // Calculate required size
  size_t required_size = (offset + count) * sizeof(int);

  // Check if we need to extend
  if (required_size > *current_size) {
    size_t page_size = sysconf(_SC_PAGESIZE);
    size_t new_size = (required_size + page_size - 1) & ~(page_size - 1);

    // extend the file size
    if (ftruncate(disk_fd, new_size) == -1) {
      perror("ftruncate failed");
      return NULL;
    }

    // Unmap the old region if it exists
    if (mapped_addr != NULL) {
      if (munmap(mapped_addr, *current_size) == -1) {
        perror("munmap failed");
        return NULL;
      }
    }

    // Create new mapping
    void *new_addr = mmap(NULL, new_size, PROT_READ | PROT_WRITE, MAP_SHARED, disk_fd, 0);
    if (new_addr == MAP_FAILED) {
      perror("mmap failed");
      return NULL;
    }

    // Update current size and address
    *current_size = new_size;
    mapped_addr = new_addr;
  }

  // Copy new values to the specified offset
  memcpy((char *)mapped_addr + (offset * sizeof(int)), new_values, count * sizeof(int));

  return mapped_addr;
}

void exec_insert(DbOperator *query, message *send_message) {
  cs165_log(stdout, "Executing insert query.\n");
  InsertOperator *insert_op = &query->operator_fields.insert_operator;

  Column *cols = insert_op->table->columns;
  size_t num_cols = insert_op->table->num_cols;
  int *values = query->operator_fields.insert_operator.values;

  for (size_t i = 0; i < num_cols; i++) {
    cs165_log(stdout, "adding %d to col %s\n", values[i], cols[i].name);
    int *new_region =
        extend_and_update_mmap((int *)cols[i].data, &cols[i].mmap_size,
                               cols[i].num_elements, &values[i], 1, cols[i].disk_fd);
    if (new_region == NULL) {
      send_message->status = EXECUTION_ERROR;
      send_message->payload = "Failed to extend and update mmap";
      send_message->length = strlen(send_message->payload);
      return;
    }
    cols[i].data = new_region;

    cols[i].num_elements++;
    cols[i].min_value = values[i] < cols[i].min_value ? values[i] : cols[i].min_value;
    cols[i].max_value = values[i] > cols[i].max_value ? values[i] : cols[i].max_value;
    cols[i].sum += values[i];
    cols[i].is_dirty = 0;
  }

  log_info("successfully added new values in table");
  send_message->status = OK_DONE;
  send_message->payload = "Done";
  send_message->length = strlen(send_message->payload);
}
