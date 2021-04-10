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
import numpy as np

##
## Only uncomment section below if ESRI updates their data
##

# dict = {
#         'UNEMPRT_CY': 'Unemployment Rate'
# }
#
# ESRI_county_unemployment_df = pd.DataFrame()
#
# # gis = GIS('https://www.arcgis.com', 'Tammy_Mason_LearnArcGIS', 'tooToo123!@#')
# gis = GIS('https://www.arcgis.com', 'arcgis_python', 'P@ssword123')
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
#     if ESRI_county_unemployment_df.empty:
#         ESRI_county_unemployment_df = data
#     else:
#         ESRI_county_unemployment_df = ESRI_county_unemployment_df.append(data)
#
#     print('# of Counties: {}'.format(len(ESRI_county_unemployment_df)))
#
# ESRI_county_unemployment_df.to_csv('RAW_ESRI_Counties_Unemployment.csv')

ESRI_county_unemployment_df = pd.read_csv('RAW_ESRI_Counties_Unemployment.csv', converters={'Geo_ID':str}).drop(columns=['Unnamed: 0'])
ESRI_county_unemployment_df['Geo_ID'] = ESRI_county_unemployment_df['Geo_ID'].apply(lambda x: x.zfill(5))


# Get BLS Data and join on ESRI data
sql = sql_caller.SqlCaller(create_tables=False)
bls_unemployment_data = sql.db_get_BLS_county_unemployment()
bls_unemployment_data['UnemploymentRate'] = bls_unemployment_data['UnemploymentRate'].astype(float)
match = pd.merge(bls_unemployment_data, ESRI_county_unemployment_df, how='inner', left_on=['Geo_ID'], right_on=['Geo_ID']).rename(columns={'UnemploymentRate_x': 'UnemploymentRate_BLS', 'UnemploymentRate_y': 'UnemploymentRate_ESRI'})

# Make sure there are no new missing ESRI IDs that we have not accounted for
arcgisids_not_in_BLS = ESRI_county_unemployment_df[(~ESRI_county_unemployment_df.Geo_ID.isin(match.Geo_ID))]
for i, row in arcgisids_not_in_BLS.iterrows():
    if row['Geo_ID'] not in ['15005']:
        arcgisids_not_in_BLS.to_excel('arcgis_missing_mlsids.xlsx')
        print('!!! WARNING - There is a new ESRI Geo ID that is not found in BLS.')
        sys.exit()

# Set multiplier
match['Unemployment_Adjustment'] = match['UnemploymentRate_BLS'] / match['UnemploymentRate_ESRI']

for i, row in match.iterrows():
    if row['Unemployment_Adjustment'] == np.inf:
        match.at[i, 'Unemployment_Adjustment'] = match.at[i, 'UnemploymentRate_BLS']
        print("!!! Found a inf number !!!")

match.to_csv('ESRI_Counties_Unemployment_Adjustments.csv')
