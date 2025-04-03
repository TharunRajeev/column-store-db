/* This line at the top is necessary for compilation on the lab machine and many other
Unix machines. Please look up _XOPEN_SOURCE for more details. As well, if your code does
not compile on the lab machine please look into this as a a source of error. */
#define _XOPEN_SOURCE
#define _POSIX_C_SOURCE 200809L  // for getline() function

/**
 * client.c
 *  CS165 Fall 2018
 *
 * This file provides a basic unix socket implementation for a client
 * used in an interactive client-server database.
 * The client receives input from stdin and sends it to the server.
 * No pre-processing is done on the client-side.
 *
 * For more information on unix sockets, refer to:
 * http://beej.us/guide/bgipc/output/html/multipage/unixsock.html
 **/
#include <ctype.h>
#include <errno.h>
#include <limits.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <sys/un.h>
#include <unistd.h>

#include "common.h"
#include "utils.h"

#define DEFAULT_STDIN_BUFFER_SIZE 1024

int connect_client(void);
int send_column_data(int socket, const char *csv_filename);

/**
 * Getting Started Hint:
 *      What kind of protocol or structure will you use to deliver your results from the
 *server to the client? What kind of protocol or structure will you use to interpret
 *results for final display to the user?
 *
 **/
int main(void) {
  int client_socket = connect_client();
  if (client_socket < 0) {
    exit(1);
  }

  message send_message;
  message recv_message;

  // Always output an interactive marker at the start of each command if the
  // input is from stdin. Do not output if piped in from file or from other fd
  char *prefix = "";
  if (isatty(fileno(stdin))) {
    prefix = "db_client > ";
  }

  char *output_str = NULL;
  int len = 0;

  // Continuously loop and wait for input. At each iteration:
  // 1. output interactive marker
  // 2. read from stdin until eof.
  char read_buffer[DEFAULT_STDIN_BUFFER_SIZE];

  while (printf("%s", prefix),
         output_str = fgets(read_buffer, DEFAULT_STDIN_BUFFER_SIZE, stdin),
         !feof(stdin)) {
    if (output_str == NULL) {
      log_err("fgets failed.\n");
      break;
    }

    // Only process input that is greater than 1 character.
    // Convert to message and send the message and the
    // payload directly to the server.
    if (strlen(read_buffer) <= 1) continue;
    if (strncmp(read_buffer, "--", 2) == 0) {
      // The -- signifies a comment line, no need to send to server
      continue;
    }

    if (strncmp(read_buffer, "shutdown", 8) == 0) {
      send_message.status = SERVER_SHUTDOWN;
      if (send(client_socket, &send_message, sizeof(message), 0) == -1) {
        log_err("Failed to send shutdown message");
      }
      exit(0);
    }

    log_client_perf(stdout, "--Query: %s", read_buffer);
    double query_t0 = get_time();

    // Check if the input is a load command
    if (strncmp(read_buffer, "load(", 5) == 0) {
      char filename[MAX_PATH_LEN];
      sscanf(read_buffer, "load(\"%[^\"]\")", filename);

      send_message.status = CSV_TRANSFER;
      // cs165_log(stdout, "sending csv transfer start message\n");
      if (send(client_socket, &send_message, sizeof(message), 0) == -1) {
        log_err("Failed to send CSV transfer start message");
        exit(1);
      }
      // cs165_log(stdout, "sending csv file\n");
      if (send_column_data(client_socket, filename) == -1) {
        log_err("Failed to send CSV file");
        exit(1);
      }
    } else {  // Should be an interesting query to leave to the server
      send_message.length = strlen(read_buffer);
      send_message.status = INCOMING_QUERY;
      // Send the message_header, which tells server payload size
      if (send(client_socket, &(send_message), sizeof(message), 0) == -1) {
        log_err("Failed to send message header.");
        exit(1);
      }

      send_message.payload = read_buffer;
      //   cs165_log(stdout, "-- sending message payload:%s\n", send_message.payload);
      // Send the payload (query) to server
      if (send(client_socket, send_message.payload, send_message.length, 0) == -1) {
        log_err("Failed to send query payload.");
        exit(1);
      }
    }

    // Always wait for server response (even if it is just an OK message)
    if ((len = recv(client_socket, &(recv_message), sizeof(message), 0)) > 0) {
      if ((recv_message.status == OK_WAIT_FOR_RESPONSE ||
           recv_message.status == OK_DONE) &&
          (int)recv_message.length > 0) {
        // Calculate number of bytes in response package
        int num_bytes = (int)recv_message.length;
        char payload[num_bytes + 1];
        // Receive the payload and print it out
        if ((len = recv_message_safe(client_socket, payload, num_bytes)) > 0) {
          log_client_perf(stdout, "--\tt = %.6fμs\n\n", recv_message.payload,
                          get_time() - query_t0);
          payload[num_bytes] = '\0';
          // TODO: refactor to only have server send payload only
          // if it was a `print` command otherwise, send just the status. (time
          // permitting)
          if (strncmp(read_buffer, "print", 5) == 0) {
            printf("%s\n", payload);
          }
        }
      }
    } else {
      if (len < 0) {
        log_err("Failed to receive message.");
      } else {
        log_info("-- Server closed connection\n");
      }
      exit(1);
    }
  }
  close(client_socket);
  return 0;
}
/**
 * connect_client()
 *
 * This sets up the connection on the client side using unix sockets.
 * Returns a valid client socket fd on success, else -1 on failure.
 *
 **/
int connect_client(void) {
  int client_socket;
  size_t len;
  struct sockaddr_un remote;

  log_info("-- Attempting to connect...\n");

  if ((client_socket = socket(AF_UNIX, SOCK_STREAM, 0)) == -1) {
    log_err("L%d: Failed to create socket.\n", __LINE__);
    return -1;
  }

  remote.sun_family = AF_UNIX;
  strncpy(remote.sun_path, SOCK_PATH, strlen(SOCK_PATH) + 1);
  len = strlen(remote.sun_path) + sizeof(remote.sun_family) + 1;
  if (connect(client_socket, (struct sockaddr *)&remote, len) == -1) {
    log_err("client connect fclailed: ");
    return -1;
  }

  log_info("-- Client connected at socket: %d.\n", client_socket);
  return client_socket;
}

char **extract_csv_columns(const char *header, int *num_columns) {
  char **columns = NULL;
  char *token;
  char header_copy[MAX_SIZE_NAME * MAX_COLUMNS];
  int count = 0;

  // Create a copy of the header string since strtok modifies the string
  strncpy(header_copy, header, sizeof(header_copy));
  header_copy[sizeof(header_copy) - 1] = '\0';  // Ensure null-termination

  // Allocate memory for the array of column names
  columns = (char **)malloc(MAX_COLUMNS * sizeof(char *));
  if (columns == NULL) {
    fprintf(stderr, "Memory allocation failed\n");
    return NULL;
  }

  // Get the first token
  token = strtok(header_copy, ",");

  // Walk through other tokens
  while (token != NULL && count < MAX_COLUMNS) {
    // Allocate memory for this column name
    columns[count] = (char *)malloc(MAX_SIZE_NAME * sizeof(char));
    if (columns[count] == NULL) {
      fprintf(stderr, "Memory allocation failed\n");
      // Free previously allocated memory
      for (int i = 0; i < count; i++) {
        free(columns[i]);
      }
      free(columns);
      return NULL;
    }

    // Copy the token to the column name; avoid including
    int i = 0;
    while (i < MAX_SIZE_NAME - 1 && token[i] != '\0' && !isspace(token[i])) {
      columns[count][i] = token[i];
      i++;
    }
    columns[count][i] = '\0';  // Ensure null-termination

    // cs165_log(stdout, "column %d: %s\n", count, columns[count]);

    count++;
    token = strtok(NULL, ",");
  }

  *num_columns = count;
  return columns;
}

/**
 * @brief send_column_data
 * Sends column data to the server
 *
 * @param socket
 * @param csv_filename
 * @return int
 */
int send_column_data(int socket, const char *csv_filename) {
  FILE *file = fopen(csv_filename, "r");
  if (!file) {
    log_err("Error opening CSV file");
    return -1;
  }

  char *line = NULL;
  size_t len = 0;
  ssize_t read;

  // Read header
  getline(&line, &len, file);
  int num_columns = 0;
  char **column_names = extract_csv_columns(line, &num_columns);
  //   cs165_log(stdout, "num of columns: %d\n", num_columns);

  int **column_data = malloc(num_columns * sizeof(int *));
  size_t *column_sizes = calloc(num_columns, sizeof(size_t));

  // count rows and allocate memory
  size_t num_rows = 0;
  while ((read = getline(&line, &len, file)) != -1) num_rows++;
  //   cs165_log(stdout, "num of rows: %zu\n", num_rows);

  ColumnMetadata *metadata = malloc(num_columns * sizeof(ColumnMetadata));
  for (int i = 0; i < num_columns; i++) {
    strncpy(metadata[i].name, column_names[i], MAX_SIZE_NAME - 1);
    metadata[i].name[MAX_SIZE_NAME - 1] = '\0';  // Ensure null-termination
    metadata[i].num_elements = 0;
    metadata[i].min_value = INT_MAX;
    metadata[i].max_value = INT_MIN;
    metadata[i].sum = 0;
    column_data[i] = malloc(num_rows * sizeof(int));
  }

  // read data and calculate metadata
  fseek(file, 0, SEEK_SET);

  // Skip header
  if (getline(&line, &len, file) == -1) {
    log_err("send_column_data: failed to read the csv header");
    return -1;
  }

  while ((read = getline(&line, &len, file)) != -1) {
    char *token = strtok(line, ",");
    for (int i = 0; i < num_columns; i++) {
      int value = atoi(token);
      column_data[i][column_sizes[i]] = value;

      // Update metadata
      if (column_sizes[i] == 0 || value < metadata[i].min_value)
        metadata[i].min_value = value;
      if (column_sizes[i] == 0 || value > metadata[i].max_value)
        metadata[i].max_value = value;
      metadata[i].sum += value;

      column_sizes[i]++;
      token = strtok(NULL, ",");
    }
  }
  //   cs165_log(stdout, "Finished reading data\n");

  // Send data for each column
  for (int i = 0; i < num_columns; i++) {
    metadata[i].num_elements = column_sizes[i];
    // log_info("Sending column %s\nWith Stats:\n\tmin: %ld\n\tmax: %ld\n\tsum: %ld\n",
    //          metadata[i].name, metadata[i].min_value, metadata[i].max_value,
    //          metadata[i].sum);

    if (send(socket, &metadata[i], sizeof(ColumnMetadata), 0) == -1) {
      log_err("Error sending metadata for column %s with error %s\n", metadata[i].name,
              strerror(errno));
      return -1;
    }
    if (send(socket, column_data[i], column_sizes[i] * sizeof(int), 0) == -1) {
      log_err("Error sending data for column %s with error %s\n", metadata[i].name,
              strerror(errno));
      return -1;
    }
  }
  // send end of transmission signal
  ColumnMetadata end_metadata = {0};
  if (send(socket, &end_metadata, sizeof(ColumnMetadata), 0) == -1) {
    log_err("Error sending end of transmission signal with error %s\n", strerror(errno));
    return -1;
  }

  // Cleanup
  for (int i = 0; i < num_columns; i++) {
    free(column_data[i]);
    free(column_names[i]);
  }
  free(column_data);
  free(column_names);
  free(metadata);
  free(column_sizes);

  fclose(file);
  //   log_info("Client finished sending data\n");
  return 0;
}
