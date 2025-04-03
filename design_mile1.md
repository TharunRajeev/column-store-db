# CS165 Proj Milestone 1 Design Doc

## Problem Statement


## Requirements
…
..

## Implementation Details

### Database Catalog Implementation

#### Metadata & Catalog Representation

- The name of database being worked on
- Pointers to all tables in a database
- Pointer to all Columns
- Metadata for each column: 
- Datatype
- Number of elements
- Basic stats: Min and max in a column (??) 


#### Approach 1: Single Metadata 

Metadata File Structure: 

```{txt}
DatabaseName

Num_tables 
tbl1_name, tbl2_name, ..., tbln_name

[tbl1.col1.name, size, datype],[], [],..., [tbl1.coln.name, size, datype]
[tbl2.col1.name, size, datype], ..., [tbl2.coln.name, size, datatype]

...
[tbl2.col1.name, size, datype], ..., [tbl2.coln.name, size, datatype]
```

#### Approach 2: Directory-oriented

Directory structure:  
```
- database_name
  - .dbmetadata
  - table_name
    - .tblmetadata
    - col1            // binary files 
    - col2
    - coln
    - ...
```

#### Tradeoff Analysis

| Single Metadata File      |  Directory-oriented      |
| -------------- | --------------- |
| Item1.1        | Item2.1         |
| Item1.2        | Item2.2         |



#### Shutdown & Startup: Strategies

1. Approach 1: Write to a file (e.x CSV) directly and parse it
2. Approach 2: Serialization 
3. Tradeoff Analysis

### Variable Pool Implementation

A structure to keep track of intermediate results for a client. For example:

```
s1 = select(db.table1.col1,null, 20)`
f1 = fetch(db.table1.col2,s1)
print(s1)
```

The role of variable pool is to keep track of `s1` and `f1`’s results and allow looking them up when needed in the next commands/queries.

#### **Approach 1**: Map/Dictionary 

A map/dictionary where keys are the variable names and values are the pointers to the results of the query (e.x. pointer to a Table object)

#### **Approach 2**: Position Lists

Have a vector (dynamically sized) to store positions of qualifying rows in the column 
Approach 3: Bit Vectors
Fixed-size (same size as column being scanned) with 1’s where the row satisfies the condition, 0 otherwise.

#### Tradeoff Analysis

| Position Lists | Bit Vectors |
| -------------- | --------------- |
| Space efficient | Uses more space|
| Implementation complexity: C doesn't give this data structure out of the box | Simple Implementation: using arrays since the size of the column is known ahead of time|
| Less efficient if intermediate results are majority of the column data* | More efficient in this case |
| Look up time: the next `fetch` operation will be more efficient if the result size (e.x. `s1`) is significantly smaller than the column being scanned | The opposite: less efficient | 


*depending on the implementation of the Position Lists (e.x. how many pages/size it mallocs at a time) data structure and the size of the column. 

**Decision:** For milestone 1, we will use bit vector for its simplicity and to prioritize have a work baseline implementation to work with. 


