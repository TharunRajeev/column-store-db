// utils.h
// CS165 Fall 2015
//
// Provides utility and helper functions that may be useful throughout.
// Includes debugging tools.

#ifndef __UTILS_H__
#define __UTILS_H__

#include <stdarg.h>
#include <stdio.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/types.h>

#include "common.h"

/**
 * trims newline characters from a string (in place)
 **/

char *trim_newline(char *str);

/**
 * trims parenthesis characters from a string (in place)
 **/

char *trim_parenthesis(char *str);

/**
 * trims whitespace characters from a string (in place)
 **/

char *trim_whitespace(char *str);

/**
 * trims quotations characters from a string (in place)
 **/

char *trim_quotes(char *str);

// cs165_log(out, format, ...)
// Writes the string from @format to the @out pointer, extendable for
// additional parameters.
//
// Usage: cs165_log(stderr, "%s: error at line: %d", __func__, __LINE__);
void cs165_log(FILE *out, const char *format, ...);

// log_err(format, ...)
// Writes the string from @format to stderr, extendable for
// additional parameters. Like cs165_log, but specifically to stderr.
//
// Usage: log_err("%s: error at line: %d", __func__, __LINE__);
void log_err(const char *format, ...);
void log_debug(const char *format, ...);
void log_perf(const char *format, ...);
void log_client_perf(FILE *out, const char *format, ...);

// log_info(format, ...)
// Writes the string from @format to stdout, extendable for
// additional parameters. Like cs165_log, but specifically to stdout.
// Only use this when appropriate (e.g., denoting a specific checkpoint),
// else defer to using printf.
//
// Usage: log_info("Command received: %s", command_string);
void log_info(const char *format, ...);

void handle_error(message *send_message, char *error_message);

ssize_t send_message_safe(int socket, const void *buffer, size_t length);
ssize_t recv_message_safe(int socket, void *buffer, size_t length);

// Get current time in microseconds
double get_time(void);

#endif /* __UTILS_H__ */
