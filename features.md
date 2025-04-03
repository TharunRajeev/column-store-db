### Create(col,"col2",db1.tbl1)

1. how to create create(db,"db1")

- create database dir and a `.metadata` file in it.
- `mmap` the `.metadata` file so that it can be used in in-memory to track creation of new tables, columns, and other metadata related to it.

2. create(tbl,"tbl1",db1,2)


3. create(col,"col1",db1.tbl1)

what to store in `.metadata` ? 
- 
- Column size: to allow  `mmap` of a column at start up. 


### Load

**Qn: How does `load` not take a name of database and table to load data to? What happens in case where we have multiple tables?** 
-> The csv file headers tells what database, table name, and column to load data to. An example header name is: `db_name.table_name.col_name`

**Qn: why happens when `load` is called twice in row?**

-> Implied from the above answer. 

**Assumptions**
-

`load` query is only passed when we have: 

- Exactly one database and one table created. 
- The number of columns in the created table is equal to the number of columns (headers) of the passed csv file. 

Examples: 
-
```
create(col,"col4",db1.tbl2)
load("/cs165/generated_data/data2_generated.csv")
```

Approach
-

1. Open the csv and get db, tbl, and col names of interest from the csv file passed
2. Use variable pool to get the pointers to the columns
3. For each row in the csv, write the add it to its respective column 


**NOTE**: we need to make sure the `mmap`-ed columns have enough size to store all data to be loaded. Otherwise, we will need to `mremap` to extend the column size. 

Data Movement Analysis
-

- 1 Pass of CSV file V/S n-passes where n is the number of columns? 


### Select

- Point-based ( <>= a)

- Range based ( a <= x <= b)


### Fetch 


### Print

print(out1,out2,out3,out4,out5)

------------------- Test 02 Pass ----------------------


### Insert 

relational_insert(db1.tbl2,-1,-11,-111,-1111)


### Aggregation 

#### Average

s1=select(db1.tbl1.col1,956,972)
f1=fetch(db1.tbl1.col2,s1)
a1=avg(f1)


--------------------- Test 03 Pass (Midway Checkin) ---

#### Sum 

-- SELECT SUM(col3) FROM tbl2 WHERE col1 >= -4554 AND col1 < 3446;
s1=select(db1.tbl2.col1,-4554,3446)
f1=fetch(db1.tbl2.col3,s1)
a1=sum(f1)
print(a1)

-- SELECT SUM(col1) FROM tbl2;
a2=sum(db1.tbl2.col1)
print(a2)


#### Add & Sub 

Add two columns

a11=add(f11,f12)
s21=sub(f22,f21)

#### Min / Max 

For example, with Max, should be able to perform: 

-- SELECT max(col1) FROM tbl2;
a2=max(db1.tbl2.col1)
print(a2)

-- SELECT max(col1) FROM tbl2 WHERE col1 >= 1622 AND col1 < 2622;
s21=select(db1.tbl2.col1,1622,2622)
f21=fetch(db1.tbl2.col1,s21)
m21=max(f21)
print(m21)

-- SELECT max(col2) FROM tbl2 WHERE col1 >= 1622 AND col1 < 2622;
f22=fetch(db1.tbl2.col2,s21)
m22=max(f22)
print(m22)

