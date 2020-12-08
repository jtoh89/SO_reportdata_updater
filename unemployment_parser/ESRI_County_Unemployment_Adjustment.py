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


# dict = {
#         'UNEMPRT_CY': 'Unemployment Rate'
# }
#
# county_unemployment_df = pd.DataFrame()
# #
# gis = GIS('https://www.arcgis.com', 'Tammy_Mason_LearnArcGIS', 'tooToo123!@#')
# # gis = GIS('https://www.arcgis.com', 'arcgis_python', 'P@ssword123')
#
# counties = pd.read_csv('ESRI_Counties.csv',converters={'COUNTYID':str})
# counties['COUNTYID'] = counties['COUNTYID'].apply(lambda x: x.zfill(5))
#
# counties_list = counties['COUNTYID']
# chunks = (len(counties_list) - 1) // 50 + 1
# for i in range(chunks):
#     print('Running Batch #{}'.format(i+1))
#     batch = counties_list[i*50:(i+1)*50]
#     variables = list(batch)
#
#     ##   CALL GEOENRICH API
#     data = enrich(study_areas=[{"sourceCountry":"US","layer":"US.Counties","ids":variables}],
#                   analysis_variables=list(dict.keys()),
#                   return_geometry=False)
#
#     data = data.drop(columns=['ID', 'apportionmentConfidence', 'OBJECTID','aggregationMethod','populationToPolygonSizeRating', 'HasData', 'sourceCountry'])
#     data = data.rename(columns={'StdGeographyLevel':'Geo_Type', 'StdGeographyID': 'Geo_ID','StdGeographyName':'Geo_Name','UNEMPRT_CY': 'UnemploymentRate'})
#
#     if county_unemployment_df.empty:
#         county_unemployment_df = data
#     else:
#         county_unemployment_df = county_unemployment_df.append(data)
#
#     print('# of Counties: {}'.format(len(county_unemployment_df)))
#
# county_unemployment_df.to_csv('RAW_ESRI_Counties_Unemployment.csv')

county_unemployment_df = pd.read_csv('RAW_ESRI_Counties_Unemployment.csv',converters={'Geo_ID':str}).drop(columns=['Unnamed: 0'])
county_unemployment_df['Geo_ID'] = county_unemployment_df['Geo_ID'].apply(lambda x: x.zfill(5))


# Get BLS Data and join on ESRI data
sql = sql_caller.SqlCaller(create_tables=True)
bls_unemployment_data = sql.db_get_BLS_county_unemployment()
bls_unemployment_data['UnemploymentRate'] = bls_unemployment_data['UnemploymentRate'].astype(float)
match = pd.merge(bls_unemployment_data, county_unemployment_df, how='left', left_on=['Geo_ID'], right_on=['Geo_ID']).rename(columns={'UnemploymentRate_x': 'UnemploymentRate_ESRI', 'UnemploymentRate_y': 'UnemploymentRate_BLS'})


# Set multiplier
match['Unemployment_Adjustment'] = match['UnemploymentRate_BLS'] / match['UnemploymentRate_ESRI']
match.to_csv('ESRI_Counties_Unemployment_Adjustments.csv')
