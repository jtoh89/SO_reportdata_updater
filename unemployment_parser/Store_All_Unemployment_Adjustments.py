from arcgis.gis import GIS
from arcgis.geocoding import geocode
from arcgis.geoenrichment import standard_geography_query
from arcgis.geoenrichment import BufferStudyArea
from arcgis.geoenrichment import enrich
import pandas as pd
import json
import requests
from db_layer import sql_caller
import sys




County_unemployment_df = pd.read_csv('ESRI_Counties_Unemployment_Adjustments.csv',converters={'Geo_ID':str}).drop(columns=['Unnamed: 0'])
County_unemployment_df['Geo_ID'] = County_unemployment_df['Geo_ID'].apply(lambda x: x.zfill(5))

MSA_Unemployment = pd.read_csv('ESRI_Msa_Unemployment_Adjustments.csv',converters={'Geo_ID':str}).drop(columns=['Unnamed: 0'])

for i, row in MSA_Unemployment.iterrows():
    if row['Geo_Type'] == 'US.States':
        MSA_Unemployment.at[i, 'Geo_ID'] = row['Geo_ID'].zfill(2)
    else:
        MSA_Unemployment.at[i, 'Geo_ID'] = row['Geo_ID'].zfill(5)


Unemployment_data = County_unemployment_df.append(MSA_Unemployment)


sql = sql_caller.SqlCaller(create_tables=True)
sql.db_dump_ESRI_Unemployment_Adjustments(Unemployment_data)


