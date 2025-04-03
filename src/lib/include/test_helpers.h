#ifndef TEST_HELPERS_H
#define TEST_HELPERS_H

#include <stdarg.h>
#include <stdio.h>
#include <sys/types.h>

void assert_nice(size_t found_i, size_t expected_i, char *endl);

#endif

void log_success(const char *format, ...) {
  va_list v;
  va_start(v, format);
  fprintf(stdout, "\x1b[32m");
  vfprintf(stdout, format, v);
  fprintf(stdout, "\x1b[0m");
  fflush(stdout);
  va_end(v);
}

void log_failure(const char *format, ...) {
  va_list v;
  va_start(v, format);
  fprintf(stdout, "\x1b[31m");
  vfprintf(stdout, format, v);
  fprintf(stdout, "\x1b[0m");
  fflush(stdout);
  va_end(v);
}

void test_title(const char *title) { fprintf(stdout, "\n\n\x1b[34m%s\x1b[0m\n", title); }
void test_sub_title(const char *title) {
  fprintf(stdout, "\n\x1b[34m%s\x1b[0m\n", title);
}

void assert_nice(size_t found_i, size_t expected_i, char *endl) {
  if (found_i != expected_i) {
    log_failure("𐄂 Expected %zu, but got %zu\n", expected_i, found_i);
  } else {
    log_success("✓ Pass%s", endl);
  }
}
