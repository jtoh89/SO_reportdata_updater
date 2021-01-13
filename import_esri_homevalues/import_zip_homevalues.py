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


dict = {
        'MEDVAL_CY': 'Median Home Value'
}

ESRI_county_medhomevalue_df = pd.DataFrame()

# gis = GIS('https://www.arcgis.com', 'Tammy_Mason_LearnArcGIS', 'tooToo123!@#')
gis = GIS('https://www.arcgis.com', 'arcgis_python', 'P@ssword123')
# gis = GIS('https://www.arcgis.com', 'OscarJuliJuli9932', 'hdf23df!erf')
# gis = GIS('https://www.arcgis.com', 'jonoh120689', 'jack1ass')



sql = sql_caller.SqlCaller(create_tables=False)
zipcodes = sql.db_get_GeoMapping_Zipcode_to_CountyMSAState()

skip_zips = pd.read_csv('ZIP_Skiplist.csv')
skip_zips = skip_zips['ZIP'].apply(lambda x: str(x).zfill(5))

zipcodes = zipcodes[~zipcodes['ZIP'].isin(skip_zips)]
zipcodes = zipcodes['ZIP'].drop_duplicates()


chunks = (len(zipcodes) - 1) // 1000 + 1
for i in range(chunks):
    print('Starting Batch #{}'.format(i+1))
    batch = zipcodes[i*1000:(i+1)*1000]
    variables = list(batch)

    skip_zips = pd.read_csv('ZIP_Skiplist.csv')
    if len(skip_zips) > 0:
        pd.DataFrame((variables + list(skip_zips['ZIP'].apply(lambda x: str(x).zfill(5)))),columns=['ZIP']).to_csv('ZIP_Skiplist.csv', index=False)
    else:
        pd.DataFrame(variables, columns=['ZIP']).to_csv('ZIP_Skiplist.csv', index=False)

    ##   CALL GEOENRICH API
    data = enrich(study_areas=[{"sourceCountry":"US","layer":"US.ZIP5","ids":variables}],
                  analysis_variables=list(dict.keys()),
                  return_geometry=False)

    data = data.drop(columns=['ID', 'apportionmentConfidence', 'OBJECTID','aggregationMethod','populationToPolygonSizeRating', 'HasData', 'sourceCountry'])
    data = data.rename(columns={'StdGeographyLevel':'Geo_Type', 'StdGeographyID': 'Geo_ID','StdGeographyName':'Geo_Name','MEDVAL_CY': 'MedianHomeValue'})

    print('{}/{} Zip Data came back'.format(len(data),len(variables)))

    if ESRI_county_medhomevalue_df.empty:
        ESRI_county_medhomevalue_df = data
    else:
        ESRI_county_medhomevalue_df = ESRI_county_medhomevalue_df.append(data)

    sql.db_dump_TEST_ZIP_IMPORT(data)
    print('Finished batch {} of {}'.format(i+1,chunks))

# ESRI_county_medhomevalue_df.to_csv('RAW_ESRI_Zip_HomeValues.csv', index=False)



