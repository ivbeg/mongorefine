# MongoRefine

This is an highly experimental data refining / wrangling package. It's inspired by [OpenRefine](https://openrefine.org) and created as headless refine replacement. The key feature is NoSQL data support using MongoDB database collections and MongoDB engine to apply data Mutations. 

Everything in this repository could change. Lack of documentation is not by mistake but is the result of experimentational nature of this code.

## Requirements

This library uses `pymongo` and `glom` Python libraries. Make sure to have them installed or use `pip install -r requirements.txt`.
You may need also `rich` and `requests` Python libraries to run examples in `examples` directory.

## Installation

Use `python setup.py install` to install.

## Examples

See more examples in [examples](https://github.com/ivbeg/mongorefine/tree/main/examples) directory.

### Character case manipulations

```python
from mongorefine import Refiner

# Collect to local database 'russianit' with collection 'pubprofile'
refiner = Refiner(dbname='resprj', collname='payments')
refiner.scan()
print(refiner.metadata())

# Get single column with project country
column = refiner.get_column('projectCountry')

# Let's measure how many records with this column exists
print(column.count())

# Lower case of column values using 'full' mode (slow but supports any encoding)
column.lower(mode='full')

# Iterate first 10 values 
print(column.items(0, 10))

# Upper case of column values using 'full' mode (slow but supports any encoding)
column.upper(mode='full')

# Iterate first 10 values 
print(column.items(0, 10))

# Upper case of column values using 'quick' mode (default). Uses built-in encoding in MongoDB database instance. 
column.upper(mode='title')

# Iterate first 10 values 
print(column.items(0, 10))

```


## Key classes

**Refiner**

Base class to work with MongoDB database collection.

Supported functions:

- **scan** - scans collection for columns metadata. Required for **columns** method
- **metadata** - returns metadata collected by scan
- **cursor** - returns collection cursor
- **get_rowset** - returns RowSet object by query
- **get_column** - returns Column object by name
- **columns** - returns list of columns (require **scan** )
- **count** - returns number of records
- **distinct** - returns distinct values of column by provided name. 
- **replace_substring** - replaces substring inside the column
- **column_string_manipulation** - applies one of string manipulations: trim, upper, lower and e.t.c
- **remove_column** - removes columns by name
- **add_column** - adds new column with static value
- **rename_column** - rename single column
- **rename_columns** - rename list of columns
- **transform_records** - transforms every record in query and replace records
- **join_columns** - joins several columns into the new single one
- **split_column** - splits columns using divider char to the number of columns
- **transform_column** - transforms every column value by query


**Column** 

Column / field level manipulation class.

Supported functions:

- **count** - returns count of records with this column 
- **trim** - removes space or chars provided from left, right or both sides of the string
- **upper** - converts values to upper case string
- **lower** - converts values to lower case string
- **title** - converts values to title string
- **items** - returns column items as an array. Could be iterated 
- **metadata** - returns column metadata
- **distinct** - returns column distinct values
- **cursor** - returns column only db cursor
- **transform** - transforms column values
- **new** - generates a new column from this one
- **concat** - concatenate each column value with prefix and/or suffix
- **number** - converts value to integer
- **string** - converts value to string
- **todate** - converts value to datetime
- **float_** - converts value to float 
- **rename** - renames a column
- **replace** - replaces column value 
- **drop** - deletes this column
- **scan** - scans column and generates it's metadata

**RowSet**

Class to manipulate with group/set of documents/rows/records.

Supported functions

- **count** - returns count of records / rows in RowSet
- **star** - marks records with *star* boolean value
- **flag** - marks records with *flag* boolean value
- **remove** - deletes all rows in RowSet
- **transform** - transforms all records in RowSet


