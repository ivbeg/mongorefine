# Examples

These examples created from extractive companies transparency project ResourceProjects.org data scraped from it's payment API endpoint https://resourceprojects.org/api/projects
This dataset has a lot of sub-documents inside each record, so it's a good example of NoSQL data.


## Requirements

Make sure to install the following Python libraries using pip: `glom`, `rich`, `pymongo`, `requests`

## Install demo database

1. Execute `gunzip data.jsonl.gz`
2. Import data to local MongoDB database `mongoimport -d resprj -c payments --drop data.jsonl` 

## Example scripts

* resproj_columns.py - prints all columns and column properties
* resproj_categories.py - scans data collection for columns and prints categorical 
* resproj_concat.py - concatenation of column values
* resproj_frequency.py - calculates fields frequencies
* resproj_joincolumns.py - joins several columns into the new one
* resproj_strings.py - string manipulatios with letter case
* resproj_distinct.py - extracts distinct values of several columns
* resproj_transformrecords.py - enriches documents with additional country data extracted from WorldBank country API
* resproj_transformcolumn.py - transforms single column using column tranforms function
* resproj_typeconv.py - type conversion functions
