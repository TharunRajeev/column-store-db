# Column Store Database

A column-oriented database management system implementation, focusing on high performance analytics workloads. This project implements a simple but effective column-store database with features like indexing, query execution, and data manipulation.

## Overview

This column-store database is designed for analytical workloads, storing data by column rather than by row. This approach provides performance benefits for analytical queries that often access a subset of columns, reducing I/O operations and improving cache locality.

### Key Features

- Column-oriented storage model
- Support for basic SQL-like operations: create, select, fetch, insert, and more
- Aggregation functions: sum, avg, min, max
- Mathematical operations: add, subtract
- B-tree indexing for improved query performance
- Client-server architecture for remote connectivity
- Memory-mapped (mmap) file I/O for efficient data access

## Project Structure

- `/src`: Core database implementation
  - `/db`: Database engine code
    - `/impl`: Implementation files (.c)
    - `/include`: Header files (.h)
  - `/lib`: Common utilities and data structures (B-tree, hash table, vector, etc.)
- `/project_tests`: Testing scripts and utilities
  - `/data_generation_scripts`: Scripts to generate test data
- `/infra_scripts`: Helper scripts for building, running, and testing
- `/docs`: Design documentation

## Getting Started

### Prerequisites

- Docker (recommended for cross-platform compatibility)
- GCC compiler and Make
- Linux or macOS environment (see Common_Errors.txt for platform-specific issues)

### Using Docker

1. Build the Docker image:
   ```
   make build
   ```
   or
   ```
   docker build -t cs165 .
   ```

2. Run the container:
   ```
   make run
   ```
   This will mount the `src` and `project_tests` directories into the container.

### Compiling and Running Manually

1. Navigate to the source directory:
   ```
   cd src
   ```

2. Compile the project:
   ```
   make
   ```

3. Run the server:
   ```
   ./server
   ```

4. In another terminal, run the client:
   ```
   ./client
   ```

## Testing

The project includes a comprehensive testing framework with milestones:

1. Run all tests:
   ```
   ./infra_scripts/run -a
   ```

2. Run a specific milestone:
   ```
   ./infra_scripts/run -m 2
   ```

3. Run a specific test:
   ```
   ./infra_scripts/run -t 5
   ```

## Project Milestones

The project is divided into several milestones:
1. Basic storage and retrieval
2. Fast Scans: Scan sharing & multi-cores
3. Indexing 
4. Joins
5. Updates

For more, see [project requirements](http://daslab.seas.harvard.edu/classes/cs165/project.html#) from class website

## Database Operations

The database supports a minimal SQL-like language:

- `create(db, "db_name")`: Create a new database
- `create(tbl, "tbl_name", db_name, num_columns)`: Create a new table
- `create(col, "col_name", db_name.tbl_name)`: Create a new column
- `load("file.csv")`: Load data from a CSV file
- `s1 = select(db1.tbl1.col1, low_val, high_val)`: Select values within a range
- `f1 = fetch(db1.tbl1.col2, s1)`: Fetch values from selected rows
- `print(var1, var2)`: Display results
- `a1 = avg(col_data)`: Calculate average
- `s1 = sum(col_data)`: Calculate sum
- `m1 = max(col_data)`: Find maximum
- `m2 = min(col_data)`: Find minimum

## Development Guidelines

When extending the codebase, follow these guidelines:
1. Keep memory management in mind - C requires explicit memory handling
2. Follow the existing code style for consistency
3. Use Valgrind to check for memory errors
4. Test thoroughly across different platforms
5. Close file descriptors properly to avoid resource leaks

## License

This project is provided as-is for educational purposes.
