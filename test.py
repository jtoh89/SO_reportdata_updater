from arcgis.gis import GIS
from arcgis.geocoding import geocode
from arcgis.geoenrichment import standard_geography_query
from arcgis.geoenrichment import BufferStudyArea
from arcgis.geoenrichment import enrich
import pandas as pd
import requests
import json
import sys
import time


stateids = ['01','02','04','05','06','08','09','10','12','13','15','16','17',
            '18','19','20','21','22','23','24','25','26','27','28','29','30',
            '31','32','33','34','35','36','37','38','39','40','41','42','44',
            '45','46','47','48','49','50','51','53','54','55','56']

dict = {
        'MEDVAL_CY': 'MedianHomeValue',
        'AVGVAL_CY': 'AverageHomeValue',
}


gis = GIS('https://www.arcgis.com', 'arcgis_python', 'P@ssword123')
final_df = pd.DataFrame()

for state in stateids:
    data = enrich(study_areas=[{"sourceCountry":"US","layer":"US.States","ids":[state]}],
                  analysis_variables=list(dict.keys()),
                  comparison_levels=['US.CBSA'],
                  return_geometry=False)

    if len(data) > 999:
        print('!!! Data may be truncated: ',  len(data))

    data = data[data.GeoType == 'US.CBSA']
    data = data.drop(columns=['ID', 'apportionmentConfidence', 'OBJECTID','aggregationMethod','StdGeographyName',
                              'populationToPolygonSizeRating', 'HasData', 'sourceCountry'])
    data = data.rename(columns={'StdGeographyLevel':'GeoType', 'StdGeographyID': 'Geo_ID'})
    data = data.rename(columns=dict)

    if final_df.empty:
        final_df = data
    else:
        final_df = final_df.append(data)


