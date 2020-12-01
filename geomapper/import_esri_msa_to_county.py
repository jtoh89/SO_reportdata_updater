from arcgis.gis import GIS
from arcgis.geocoding import geocode
from arcgis.geoenrichment import standard_geography_query
from arcgis.geoenrichment import BufferStudyArea
from arcgis.geoenrichment import enrich
import pandas as pd
import json
import requests
import sys
import math
from sqlalchemy import create_engine
import os
import time
from db_layer import sql_caller

##
## Get list of CBSA IDs from https://hub.arcgis.com/datasets/usdot::core-based-statistical-areas/data
## Download file called Core_Based_Statistical_Areas.csv
##


sql = sql_caller.SqlCaller(create_tables=False)
msa_countystate = sql.db_get_MSA_to_CountyState()

path = os.path.dirname(os.path.abspath(__file__))
with open(path + '/Core_Based_Statistical_Areas.csv') as file:
    msa_df = pd.read_csv(file, dtype=str)

msa_count = len(msa_df)

msa_df = msa_df[~msa_df['GEOID'].isin(msa_countystate['MSAID'].drop_duplicates())]

print('Out of {} MSAs, {} are remaining'.format(msa_count, len(msa_df)))

gis = GIS('https://www.arcgis.com', 'Tammy_Mason_LearnArcGIS', 'tooToo123!@#')


msa_to_county_df = pd.DataFrame(columns=['ID','MSAID', 'AreaID', 'STATEID'])

t1 = time.time()
count = 1
for msaid in msa_df['GEOID']:
    print('Query {}'.format(count))

    geoarea = standard_geography_query(source_country='US', ids=[msaid], layers=["US.CBSA"],
                                       return_sub_geography=True,
                                       sub_geography_layer="US.Counties", generalization_level=6,
                                       return_geometry=False)

    if geoarea.empty:
        continue

    geoarea['STATEID'] = geoarea['AreaID'].str[:2]
    geoarea['MSAID'] = msaid
    geoarea['ID'] = geoarea['MSAID'] + geoarea['AreaID']

    msa_to_county_df = msa_to_county_df.append(geoarea[['ID','MSAID', 'AreaID', 'STATEID']])

    count += 1

    if count > 100:
        break

t2 = time.time()
print('Run took {}'.format(t2-t1))

sql.db_dump_MSA_to_CountyState(msa_to_county_df.rename(columns={'AreaID':'COUNTYID'}))
