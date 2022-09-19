# -*- coding: utf-8 -*-
import logging
import requests
import glom
from rich import print
from rich.table import Table

logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.DEBUG)

from mongorefine import Refiner


WORLDBANK_ENDPOINT = 'http://api.worldbank.org/v2/country/%s?format=json'

def get_cached_country_data(iso_codes):
    """Extracts country data from Worldbank country API"""
    results = {}
    for code in iso_codes:
        data = requests.get(WORLDBANK_ENDPOINT % code)
        results[code] = data.json()[1][0]
    return results
                               

refiner = Refiner(dbname='resprj', collname='payments')
refiner.scan()
#print(refiner.metadata())
columns = refiner.columns()

isocodes = refiner.get_column('source.iso')

print('Get and print country ISO codes distinct values')
isocodes_unique = isocodes.distinct()
print(isocodes_unique)

country_data = get_cached_country_data(isocodes_unique)
print(country_data)

def enrich_record_with_country_info(record):
    try:
        # We use glom to get and to assign values since they could be inside sub-documents.
        iso_code = glom.glom(record, 'source.iso')
        glom.assign(record, 'source.countryInfo', country_data[iso_code])
    except glom.core.PathAccessError:
        pass
    return record

refiner.transform_records({}, enrich_record_with_country_info)
refiner.scan()

regions = refiner.get_column('source.countryInfo.region.value')
print(regions.distinct())