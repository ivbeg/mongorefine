# -*- coding: utf-8 -*-
import logging
from pprint import pprint
from rich import print
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.DEBUG)

from mongorefine import Refiner

def get_regcode(inn):
    return inn[0:2] if inn else inn
        

refiner = Refiner(dbname='russianit', collname='pubprofile')
refiner.scan()
print(refiner.metadata())
columns = refiner.columns()
print(columns)
for column in columns:
    print(column.count())
#refiner.join_columns('merged_name', columns=['data.name', 'data.nationality'], sep='/', delete_joined=False)
#column = refiner.get_column('data.links.self')
#column.concat(prefix='https://find-and-update.company-information.service.gov.uk', destination='url')
#refiner.replace_substring('orgname', 'УУУ', 'ООО')
#column = refiner.add_column('end_order_date', '15')
#column.rename('_the_end')
#column.drop()
#column = refiner.get_column('data.name')
#column.scan()
#print(column.metadata())
#column.upper()
#new_column = column.new('regcode', function=get_regcode)
#column = refiner.get_column('territory')
#column.upper()
#column.lower()
#column.title()
