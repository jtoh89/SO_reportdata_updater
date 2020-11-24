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


df = pd.DataFrame(columns=['MSAID','CBSAID'])

Esri_to_NECTAID_conversion = {
'12620':'70750',
'12700':'70900',
'12740':'71050',
'13540':'71350',
'13620':'71500',
'14460':'71650','14860':'71950','15540':'72400','18180':'72700','19430':'19380','25540':'73450',
'28300':'73750','29060':'73900','30100':'74350','30340':'74650','31700':'74950','35300':'75700','35980':'76450','36837':'36860','38340':'76600','38860':'76750','39150':'39140',
'39300':'77200','40860':'77650','44140':'78100','45860':'78400','47240':'78500','49060':'11680','49340':'79600'
}


for k,v in Esri_to_NECTAID_conversion.items():
    df = df.append({'CBSAID':k,'MSAID': v}, ignore_index=True)

df.to_csv('test.csv')



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


