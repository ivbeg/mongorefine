# -*- coding: utf-8 -*-
import logging
from rich import print
from rich.table import Table

logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.DEBUG)

from mongorefine import Refiner
                                                          


refiner = Refiner(dbname='resprj', collname='payments')
refiner.scan()


# Original column values (should be title string by default)
projectCountry = refiner.get_column('projectCountry')
print('projectCountry column: original first 10 values')
print(projectCountry.items(0, 10))

# Convert column values to lower case
projectCountry.lower()
projectCountry.scan()
print('projectCountry column: first 10 values after lower case and rescan')
print(projectCountry.items(0, 10))

# Convert column values to upper case
projectCountry.upper()
projectCountry.scan()
print('projectCountry column: first 10 values after upper case and rescan')
print(projectCountry.items(0, 10))

# Convert column values to title string
projectCountry.title()
projectCountry.scan()
print('projectCountry column: first 10 values after title and rescan')
print(projectCountry.items(0, 10))


# Replace Algeria country short name to full name
projectCountry.replace("Algeria", "People's Democratic Republic of Algeria")
print('projectCountry column: first 10 values after replace and rescan')
print(projectCountry.items(0, 10))


# Replacing Algeria country name back to short name
projectCountry.replace("People's Democratic Republic of Algeria", "Algeria")
print('projectCountry column: first 10 values after replacing back and rescan')
print(projectCountry.items(0, 10))

