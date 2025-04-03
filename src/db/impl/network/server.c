/*
Copyright (c) 2015 Harvard University - Data Systems Laboratory (DASLab)
Permission is hereby granted, free of charge, to any person obtaining a copy of
this software and associated documentation files (the "Software"), to deal in
the Software without restriction, including without limitation the rights to
use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies
of the Software, and to permit persons to whom the Software is furnished to do
so, subject to the following conditions:
The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.
THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
*/

/** server.c
 * CS165 Fall 2024
 *
 * This file provides a basic unix socket implementation for a server
 * used in an interactive client-server database.
 * The client should be able to send messages containing queries to the
 * server.  When the server receives a message, it must:
 * 1. Respond with a status based on the query (OK, UNKNOWN_QUERY, etc.)
 * 2. Process any appropriate queries, if applicable.
 * 3. Return the query response to the client.
 *
 * For more information on unix sockets, refer to:
 * http://beej.us/guide/bgipc/output/html/multipage/unixsock.html
 **/
#include <errno.h>
#include <fcntl.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/un.h>
#include <unistd.h>

#include "algorithms.h"
#include "catalog_manager.h"
#include "client_context.h"
#include "common.h"
#include "handler.h"
#include "optimizer.h"
#include "utils.h"

#define DEFAULT_QUERY_BUFFER_SIZE 1024

int client_id = 0;
int session_id = 0;

int receive_columns(int client_socket, message *send_message);

/**
 * handle_client(client_socket)
 * This is the execution routine after a client has connected.
 * It will continually listen for messages from the client and execute queries.
 **/
void handle_client(int client_socket, int *shutdown) {
  int length = 0;
  log_info("Connected to socket: %d.\n", client_socket);
  session_id++;

  // Create two messages, one from which to read and one from which to receive
  message send_message = {.status = OK_WAIT_FOR_RESPONSE, .length = 0, .payload = NULL};
  message recv_message = {.status = OK_WAIT_FOR_RESPONSE, .length = 0, .payload = NULL};

  // create the client context here
  ClientContext *client_context = NULL;
  client_context = g_client_context;

  // Continually receive messages from client and execute queries.
  // 1. Parse the command
  // 2. Handle request if appropriate
  // 3. Send status of the received message (OK, UNKNOWN_QUERY, etc)
  // 4. Send response to the request.
  while (true) {
    length = recv(client_socket, &recv_message, sizeof(message), 0);
    if (length <= 0) {
      cs165_log(stdout, "Client connection closed!\n");
      break;
    }
    if (recv_message.status == SERVER_SHUTDOWN) {
      *shutdown = 1;
      break;
    }

    if (recv_message.status == CSV_TRANSFER)
      receive_columns(client_socket, &send_message);

    if (recv_message.status == INCOMING_QUERY) {
      char recv_buffer[recv_message.length + 1];
      length = recv(client_socket, recv_buffer, recv_message.length, 0);
      recv_message.payload = recv_buffer;
      recv_message.payload[recv_message.length] = '\0';

      cs165_log(stdout, "Received message: %s\n", recv_message.payload);

      handle_query(recv_message.payload, &send_message, client_socket, client_context);
    }

    // 3. Send status of the received message (OK, UNKNOWN_QUERY, etc)
    if (send(client_socket, &(send_message), sizeof(message), 0) == -1) {
      log_err("Failed to send message with error: %s\n", strerror(errno));
    }

    // 4. Send response to the request
    if (send_message_safe(client_socket, send_message.payload, send_message.length) ==
        -1) {
      log_err("Failed to send message.");
    }
  }
  log_info("Connection closed at socket %d!\n", client_socket);
  close(client_socket);
}

/**
 * setup_server()
 *
 * This sets up the connection on the server side using unix sockets.
 * Returns a valid server socket fd on success, else -1 on failure.
 **/
int setup_server(void) {
  int server_socket;
  size_t len;
  struct sockaddr_un local;

  log_info("Attempting to setup server...\n");

  if ((server_socket = socket(AF_UNIX, SOCK_STREAM, 0)) == -1) {
    log_err("L%d: Failed to create socket.\n", __LINE__);
    return -1;
  }

  local.sun_family = AF_UNIX;
  strncpy(local.sun_path, SOCK_PATH, strlen(SOCK_PATH) + 1);
  unlink(local.sun_path);

  /*
  int on = 1;
  if (setsockopt(server_socket, SOL_SOCKET, SO_REUSEADDR, (char *)&on,
  sizeof(on)) < 0)
  {
      log_err("L%d: Failed to set socket as reusable.\n", __LINE__);
      return -1;
  }
  */

  len = strlen(local.sun_path) + sizeof(local.sun_family) + 1;
  if (bind(server_socket, (struct sockaddr *)&local, len) == -1) {
    log_err("L%d: Socket failed to bind.\n", __LINE__);
    return -1;
  }

  if (listen(server_socket, 5) == -1) {
    log_err("L%d: Failed to listen on socket.\n", __LINE__);
    return -1;
  }

  // after all setup, setup db. TODO: check implications of this on concurrent clients
  db_startup();

  return server_socket;
}

// Currently this main will setup the socket and accept a single client.
// After handling the client, it will exit.
// You WILL need to extend this to handle MULTIPLE concurrent clients
// and remain running until it receives a shut-down command.
//
// Getting Started Hints:
//      How will you extend main to handle multiple concurrent clients?
//      Is there a maximum number of concurrent client connections you will
//      allow? What aspects of siloes or isolation are maintained in your
//      design? (Think `what` is shared between `whom`?)
int main(void) {
  int server_socket = setup_server();
  if (server_socket < 0) {
    exit(1);
  }

  log_info("Waiting for a connection %d ...\n", server_socket);

  struct sockaddr_un remote;
  socklen_t t = sizeof(remote);
  int client_socket = 0;
  int shutdown = 0;
  while (!shutdown) {
    if ((client_socket = accept(server_socket, (struct sockaddr *)&remote, &t)) == -1) {
      log_err("L%d: Failed to accept a new connection.\n", __LINE__);
    }
    handle_client(client_socket, &shutdown);
  }
  db_shutdown();
  return 0;
}

int receive_columns(int socket, message *send_message) {
  log_info("Server: Receiving column data from client at socket %d\n", socket);
  ColumnMetadata metadata = {0};
  ssize_t bytes_received = 0;
  Table *table = NULL;
  Column *primary_col = NULL;    // Primary column for indexing, this is the first column
                                 // with clustered index
  Column *secondary_col = NULL;  // Secondary column for indexing, this is unclustered
                                 // column

  while ((bytes_received = recv(socket, &metadata, sizeof(ColumnMetadata), 0)) > 0) {
    if (bytes_received != sizeof(ColumnMetadata)) {
      log_err("Error receiving metadata: expected %zu bytes, got %zd\n",
              sizeof(ColumnMetadata), bytes_received);
      return -1;
    }

    // Check for end of transmission signal
    if (metadata.num_elements == 0) {
      cs165_log(stdout, "Received end of transmission signal\n");
      break;
    }

    cs165_log(stdout, "Received metadata for column %s\n", metadata.name);
    Column *col = get_column_from_catalog(metadata.name);

    // extract table name from column name. e.g. metadata.name=db1.tbl1.col1 -> tbl1
    char table_name[strlen(metadata.name) + 1];
    strcpy(table_name, metadata.name);
    char *table_name_ptr = strtok(table_name, ".");  // first call gets "db1"
    table_name_ptr = strtok(NULL, ".");              // second call gets "tbl1"

    if (!table && table_name_ptr) table = get_table_from_catalog(table_name_ptr);
    if (!col || !table) {
      log_err("Failed to find table and column for metadata %s\n", metadata.name);
      return -1;
    }
    col->num_elements = metadata.num_elements;
    col->min_value = metadata.min_value;
    col->max_value = metadata.max_value;
    col->sum = metadata.sum;
    col->data_type = INT;

    // Calculate file size
    size_t file_size = metadata.num_elements * sizeof(int);
    col->mmap_size = file_size;

    // Construct file path
    char file_path[MAX_PATH_LEN];
    snprintf(file_path, MAX_PATH_LEN, "disk/%s.bin", metadata.name);

    // Open file
    col->disk_fd = open(file_path, O_RDWR | O_CREAT | O_TRUNC, 0644);
    if (col->disk_fd == -1) {
      log_err("Failed to open file for column %s: %s\n", metadata.name, strerror(errno));
      return -1;
    }

    // Extend file to desired size
    if (ftruncate(col->disk_fd, file_size) == -1) {
      log_err("Failed to extend file for column %s: %s\n", metadata.name,
              strerror(errno));
      close(col->disk_fd);
      return -1;
    }

    cs165_log(stdout, "Successfully created file for column %s. Trying to mmap it\n",
              metadata.name);
    // Memory map the file
    col->data =
        mmap(NULL, file_size, PROT_READ | PROT_WRITE, MAP_SHARED, col->disk_fd, 0);
    if (col->data == MAP_FAILED) {
      log_err("Failed to mmap file for column %s: %s\n", metadata.name, strerror(errno));
      close(col->disk_fd);
      continue;
    }

    // Receive column data
    size_t total_received = 0;
    while (total_received < file_size) {
      bytes_received = recv(socket, (char *)col->data + total_received,
                            file_size - total_received, MSG_WAITALL);
      if (bytes_received <= 0) {
        if (bytes_received == 0) {
          log_err("Connection closed while receiving data for column %s\n",
                  metadata.name);
        } else {
          log_err("Error receiving data for column %s: %s\n", metadata.name,
                  strerror(errno));
        }
        break;
      }
      total_received += bytes_received;
    }

    if (total_received != file_size) {
      log_err("Incomplete data received for column %s: expected %zu bytes, got %zu\n",
              metadata.name, file_size, total_received);
      return -1;
    }

    IndexType idx_type = col->index ? col->index->idx_type : NONE;
    if (idx_type != NONE) {
      if (!primary_col && (idx_type == SORTED_CLUSTERED || idx_type == BTREE_CLUSTERED)) {
        primary_col = col;
      }
      if (!secondary_col &&
          (idx_type == SORTED_UNCLUSTERED || idx_type == BTREE_UNCLUSTERED)) {
        secondary_col = col;
      }
    }
    col->is_dirty = 0;
    // NOTE: Differing this for `shutdown`
    // // Ensure data is written to disk
    // if (msync(col->data, file_size, MS_SYNC) == -1) {
    //   log_err("Failed to sync mmap'd file for column %s: %s\n", metadata.name,
    //           strerror(errno));
    //   return -1;
    // }

    log_info("Successfully received and stored data for column %s\n", metadata.name);
  }

  if (!primary_col) return 0;
  create_idx_on(primary_col, send_message);
  // TODO: debug why this messes up correctness on grading server. particularly,
  // Benchmark3
  cluster_idx_on(table, primary_col, send_message);

  if (secondary_col) create_idx_on(secondary_col, send_message);

  return 0;
}
