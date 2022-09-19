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


# Lets create new column with URL to Worldbank country API using ISO code
isocodes = refiner.get_column('source.iso')
isocodes.concat(destination='source.wbcountryURL', prefix='http://api.worldbank.org/v2/country/', postfix='?format=json')

# Lets get column data and print it's metadata and top 10 values
wbcountryURL = refiner.get_column('source.wbcountryURL')
wbcountryURL.scan()
print('Metadata of the source.wbcountryURL')
print(wbcountryURL.metadata())
print('Top 10 values of the source.wbcountryURL')
print(wbcountryURL.items(0, 10))


# Lets drop this column
print('Drop column source.wbcountryURL')
wbcountryURL.drop()
del wbcountryURL
# Let's make sure that 
refiner.scan()
if 'wbcountryURL' not in refiner.metadata()['colspecs'].keys():
    print('wbcountryURL successfully deleted from documents')
else:
    print('wbcountryURL NOT deleted from documents')

