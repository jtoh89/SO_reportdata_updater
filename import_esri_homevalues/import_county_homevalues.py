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
# gis = GIS('https://www.arcgis.com', 'arcgis_python', 'P@ssword123')
# gis = GIS('https://www.arcgis.com', 'OscarJuliJuli9932', 'hdf23df!erf')


counties = pd.read_csv('ESRI_Counties.csv',converters={'COUNTYID':str})
counties['COUNTYID'] = counties['COUNTYID'].apply(lambda x: x.zfill(5))

counties_list = counties['COUNTYID']
chunks = (len(counties_list) - 1) // 50 + 1
for i in range(chunks):
    print('Running Batch #{}'.format(i+1))
    batch = counties_list[i*50:(i+1)*50]
    variables = list(batch)

    ##   CALL GEOENRICH API
    data = enrich(study_areas=[{"sourceCountry":"US","layer":"US.Counties","ids":variables}],
                  analysis_variables=list(dict.keys()),
                  return_geometry=False)

    data = data.drop(columns=['ID', 'apportionmentConfidence', 'OBJECTID','aggregationMethod','populationToPolygonSizeRating', 'HasData', 'sourceCountry'])
    data = data.rename(columns={'StdGeographyLevel':'Geo_Type', 'StdGeographyID': 'Geo_ID','StdGeographyName':'Geo_Name','MEDVAL_CY': 'MedianHomeValue'})

    if ESRI_county_medhomevalue_df.empty:
        ESRI_county_medhomevalue_df = data
    else:
        ESRI_county_medhomevalue_df = ESRI_county_medhomevalue_df.append(data)

    print('# of Counties: {}'.format(len(ESRI_county_medhomevalue_df)))

ESRI_county_medhomevalue_df.to_csv('RAW_ESRI_Counties_HomeValues.csv', index=False)


