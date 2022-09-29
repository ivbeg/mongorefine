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

isocodes = refiner.get_column('reportingCompany')

freqcursor = isocodes.freq()

table = Table(title="Frequency of reportingCompany field values")
table.add_column("N", justify="right", style="magenta", no_wrap=True)
table.add_column("Value", justify="right", style="cyan", no_wrap=True)
table.add_column("Count", justify="right", style="green")
n = 0
for row in freqcursor:
    n += 1
    table.add_row(str(n), str(row['_id']), str(row['count']))

print(table)
