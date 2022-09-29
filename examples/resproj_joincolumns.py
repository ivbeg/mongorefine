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



# Let's join projectName, projectCountry and reportingCompany as single full name of the project using '/' separator with spaces and keep old columns 
refiner.join_columns(destination='fullName', columns=['projectName', 'projectCountry', 'reportingCompany'], sep = ' / ', delete_joined=False)
#refiner.scan()
fullName = refiner.get_column('fullName')
fullName.scan()
print('fullName top 10 values')
print(fullName.items(0, 10))
fullName.drop()
print('Column fullName deleted')
