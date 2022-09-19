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


report_start = refiner.get_column('reportStartDate')
print('report_start column: metadata')
print(report_start.metadata())
print('report_start column: original first 10 values')
print(report_start.items(0, 10))

report_start.todate()
report_start.scan()
print('report_start column: metadata after type conversion and rescan')
print(report_start.metadata())
print('report_start column: first 10 values after type conversion and rescan')
print(report_start.items(0, 10))


report_start.string()
report_start.scan()
print('report_start column: metadata after type reverting type back and rescan')
print(report_start.metadata())
print('report_start column: first 10 values after type reverting type and rescan')
print(report_start.items(0, 10))

