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
#print(refiner.metadata())
columns = refiner.columns()

isocodes = refiner.get_column('source.iso')

print('Country ISO codes distinct values')
print(isocodes.distinct())


companies = refiner.get_column('reportingCompany')

print('Companies distinct values')
print(companies.distinct())