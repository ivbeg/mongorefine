# Examples

These examples created from extractive companies transparency project ResourceProjects.org data scraped from it's payment API endpoint https://resourceprojects.org/api/projects


## Requirements

Make sure to install the following Python libraries using pip: `glom`, `rich`, `pymongo`, `requests`

## Install demo database

1. Execute `gunzip data.jsonl.gz`
2. Import data to local MongoDB database `mongoimport -d resprj -c payments --drop data.jsonl` 

## Example scripts

* resproj_columns.py - prints all columns and column properties
* resproj_categories.py - scans data collection for columns and prints categorical 
* resproj_distincy.py - extracts distinct values of several columns
* resproj_transform.py - enriches documents with additional country data extracted from WorldBank country API