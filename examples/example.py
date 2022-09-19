from rich import print
from mongorefine import Refiner
refiner = Refiner(dbname='russianit', collname='pubprofile')
refiner.scan()
print(refiner.metadata())

column = refiner.get_column('addr.city_name')
print(column.count())
column.lower()
print(column.items(0, 10))
column.lower(mode='full')
print(column.items(0, 10))
column.upper(mode='full')
print(column.items(0, 10))


