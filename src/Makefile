# CS165 MakeFile
# This file includes automatic dependency tracking
# Please see example of "utils" to see how to add additional file
# to your project

# Main targets
all: client server testlib

# C-compiler settings
CC = gcc -std=c99 -g -ggdb3

# Default optimization level
O ?= 3

# Directory structure
DB_DIR = db
LIB_DIR = lib
DB_IMPL_DIR = $(DB_DIR)/impl
LIB_IMPL_DIR = $(LIB_DIR)/impl
LIB_TST_DIR = $(LIB_DIR)/tests

# Include directories - add all subdirectories
DB_INCLUDES = $(DB_DIR)/include \
              $(DB_DIR)/include/core \
              $(DB_DIR)/include/query \
              $(DB_DIR)/include/query/executor \
              $(DB_DIR)/include/query/parser \
              $(DB_DIR)/include/utils

LIB_INCLUDES = $(LIB_DIR)/include

INCLUDES = $(DB_INCLUDES) $(LIB_INCLUDES)

# Source directories
CORE_DIR = $(DB_IMPL_DIR)/core
NETWORK_DIR = $(DB_IMPL_DIR)/network
QUERY_DIR = $(DB_IMPL_DIR)/query
QUERY_PARSER_DIR = $(QUERY_DIR)/parser
QUERY_EXECUTOR_DIR = $(QUERY_DIR)/executor
UTILS_DIR = $(DB_IMPL_DIR)/utils

SANITIZER_FLAGS = # -fsanitize=address -fsanitize=undefined -fsanitize=leak

# Flags and other libraries
override CFLAGS += -Wall -Wextra -pedantic -pthread -O$(O) $(addprefix -I,$(INCLUDES)) $(SANITIZER_FLAGS)
LDFLAGS =
LIBS =


# this is the DB server and client's unix socket path
# Note: the extra backslash is for escaping the quotes for makefile format
SOCK_PATH=\"/tmp/cs165_unix_socket\"

####### Automatic dependency magic #######
# Set-up dependency directory
DEPSDIR := .deps
BUILDSTAMP := $(DEPSDIR)/rebuildstamp
DEPFILES := $(wildcard $(DEPSDIR)/*.d)
ifneq ($(DEPFILES),)
include $(DEPFILES)
endif
DEPCFLAGS = -MD -MF $(DEPSDIR)/$*.d -MP -DSOCK_PATH=$(SOCK_PATH)

# Dependency compilation
ifneq ($(DEP_CC),$(CC) $(CFLAGS) $(DEPCFLAGS) $(O))
DEP_CC := $(shell mkdir -p $(DEPSDIR); echo >$(BUILDSTAMP); echo "DEP_CC:=$(CC) $(CFLAGS) $(DEPCFLAGS) $(O)" >$(DEPSDIR)/_cc.d)
endif

# Make sure dependency directories are generated
$(DEPSDIR)/stamp $(BUILDSTAMP):
	mkdir -p $(@D)
	touch $@

####### Source Files #######
# Core components
CORE_SRCS = $(wildcard $(CORE_DIR)/*.c)
CORE_OBJS = $(CORE_SRCS:.c=.o)

# Network components
NETWORK_SRCS = $(wildcard $(NETWORK_DIR)/*.c)
NETWORK_OBJS = $(NETWORK_SRCS:.c=.o)

# Query components (including parser and executor)
QUERY_SRCS = $(wildcard $(QUERY_DIR)/*.c)
QUERY_PARSER_SRCS = $(wildcard $(QUERY_PARSER_DIR)/*.c)
QUERY_EXECUTOR_SRCS = $(wildcard $(QUERY_EXECUTOR_DIR)/*.c)
QUERY_OBJS = $(QUERY_SRCS:.c=.o) \
             $(QUERY_PARSER_SRCS:.c=.o) \
             $(QUERY_EXECUTOR_SRCS:.c=.o)

# Utils components
UTILS_SRCS = $(wildcard $(UTILS_DIR)/*.c)
UTILS_OBJS = $(UTILS_SRCS:.c=.o)

# Library components
LIB_SRCS = $(wildcard $(LIB_IMPL_DIR)/*.c)
LIB_OBJS =  $(LIB_SRCS:.c=.o)

LIB_TSTS_SRCS = $(wildcard $(LIB_TST_DIR)/*.c)
LIB_TSTS_OBJS = $(LIB_TSTS_SRCS:.c=.o)

####### Build Rules #######
# Create .deps subdirectories to match source tree
$(shell mkdir -p $(DEPSDIR)/$(DB_IMPL_DIR)/core \
                 $(DEPSDIR)/$(DB_IMPL_DIR)/network \
                 $(DEPSDIR)/$(DB_IMPL_DIR)/query \
                 $(DEPSDIR)/$(DB_IMPL_DIR)/query/parser \
                 $(DEPSDIR)/$(DB_IMPL_DIR)/query/executor \
                 $(DEPSDIR)/$(DB_IMPL_DIR)/utils \
                 $(DEPSDIR)/$(LIB_IMPL_DIR) \
				 $(DEPSDIR)/$(LIB_TST_DIR))

%.o : %.c $(BUILDSTAMP)
	@mkdir -p $(dir $(DEPSDIR)/$*)
	$(CC) $(CFLAGS) $(DEPCFLAGS) -O$(O) -o $@ -c $<

# Print variables for debugging
print-%:
	@echo $* = $($*)

# Client executable
client: $(NETWORK_DIR)/client.o $(UTILS_OBJS) $(LIB_OBJS)
	$(CC) $(CFLAGS) $(DEPCFLAGS) -o $@ $^ $(LDFLAGS) $(LIBS)

# Server executable
server: $(NETWORK_DIR)/server.o $(CORE_OBJS) $(QUERY_OBJS) $(UTILS_OBJS) $(LIB_OBJS)
	$(CC) $(CFLAGS) $(DEPCFLAGS) -o $@ $^ $(LDFLAGS) $(LIBS)

testlib: $(LIB_TSTS_OBJS) $(UTILS_OBJS) $(LIB_OBJS)
	$(CC) $(CFLAGS) $(DEPCFLAGS) -o $@ $^ $(LDFLAGS) $(LIBS)

clean:
	rm -f client server $(CORE_OBJS) $(NETWORK_OBJS) $(QUERY_OBJS) $(UTILS_OBJS) $(LIB_OBJS) $(LIB_TSTS_OBJS)
	rm -f *~ *.bak core *.core $(SOCK_PATH)
	rm -rf .deps

distclean: clean
	rm -rf $(DEPSDIR)
	rm -rf disk/
	rm -f *.bin

.PHONY: all clean distclean
