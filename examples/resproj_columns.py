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

table = Table(title="Categorical columns")
table.add_column("Name", justify="right", style="cyan", no_wrap=True)
table.add_column("Column type", style="blue")
table.add_column("Is categorical", style="cyan")
table.add_column("Unique count", style="magenta")
table.add_column("Unique ratio", justify="right", style="magenta")
table.add_column("Row count", justify="right", style="green")
for c in columns:
    metadata = c.metadata()
    table.add_row(c.name, metadata['datatype'], str(metadata['categorical']), str(metadata['unique_count']), '%0.4f' % metadata['unique_ratio'], str(metadata['row_count']))

print(table)
