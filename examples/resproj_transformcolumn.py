# -*- coding: utf-8 -*-
import logging
from rich import print

logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.DEBUG)

from mongorefine import Refiner
                                                          


refiner = Refiner(dbname='resprj', collname='payments')
refiner.scan()

def do_reverse(s):
    return s[::-1]

def do_swapcase(s):
    return s.swapcase()

# Lets see original values
projectCountry = refiner.get_column('projectCountry')
print('projectCountry: top 10 values before changes')
print(projectCountry.items(0, 10))

# Lets do swapcase of each value and print it
projectCountry.transform({}, do_swapcase)
print('projectCountry: top 10 values after swapcase')
print(projectCountry.items(0, 10))
projectCountry.transform({}, do_swapcase) 

# Lets do reverse strings
projectCountry.transform({}, do_reverse)
print('projectCountry: top 10 values after reverse')
print(projectCountry.items(0, 10))

# Lets do reverse one more time and get original values
projectCountry.transform({}, do_reverse)
print('projectCountry: top 10 values after second reverse')
print(projectCountry.items(0, 10))
