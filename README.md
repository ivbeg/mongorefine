# MongoRefine

This is an highly experimental data refining / wrangling package. It's inspired by [OpenRefine](https://openrefine.org) and created as headless refine replacement. The key feature is NoSQL data support using MongoDB database collections and MongoDB engine to apply data Mutations. 

Everything in this repository could change. Lack of documentation is not by mistake but is the result of experimentational nature of this code.

## Requirements

This library uses `pymongo` and `glom` Python libraries. Make sure to have them installed or use `pip install -r requirements.txt`

## Installation

Use `python setup.py install` to install

## Key classes

**Refiner**

Base class to work with MongoDB database collection.

Supported functions:

- metadata
- scan
- cursor
- get_rowset
- get_column
- columns
- count
- distinct
- replace_substing
- column_string_manipulation
- remove_column
- add_column
- rename_column
- rename_columns
- transform_records
- join_columns
- split_column
- transform_column


**Column** 

Column / field level manipulation class.

Supported functions:

- count
- trim
- upper
- lower
- title
- items
- metadata
- distinct
- cursor
- transform
- new
- concat
- number
- string
- todate
- float_
- rename
- replace
- drop
- scan

**RowSet**

Class to manipulate with group/set of documents/rows/records.

Supported functions

- count
- star
- flag
- remove
- transform

## Examples


```python
from mongorefine import Refiner

# Collect to local database 'russianit' with collection 'pubprofile'
refiner = Refiner(dbname='russianit', collname='pubprofile')
refiner.scan()
print(refiner.metadata())

# Get single column with city name inside addr (address) subdocument
column = refiner.get_column('addr.city_name')

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

# Upper case of column values using 'full' mode (slow but supports any encoding)
column.upper(mode='title')

# Iterate first 10 values 
print(column.items(0, 10))

```

