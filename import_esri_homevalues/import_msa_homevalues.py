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

# gis = GIS('https://www.arcgis.com', 'Tammy_Mason_LearnArcGIS', 'tooToo123!@#')
gis = GIS('https://www.arcgis.com', 'arcgis_python', 'P@ssword123')


##   CALL GEOENRICH API
data = enrich(study_areas=[{"sourceCountry":"US","layer":"US.States","ids":['01','02','04','05','06','08','09','10','11','12','13','15','16',
                                                                            '17','18','19','20','21','22','23','24','25','26','27','28','29',
                                                                            '30','31','32','33','34','35','36','37','38','39','40','41','42',
                                                                            '44','45','46','47','48','49','50','51','53','54','55','56']}],
              analysis_variables=list(dict.keys()),
              comparison_levels=['US.CBSA','US.WholeUSA'],
              return_geometry=False)

data = data.drop(columns=['ID', 'apportionmentConfidence', 'OBJECTID','aggregationMethod','populationToPolygonSizeRating', 'HasData', 'sourceCountry'])
data = data.rename(columns={'StdGeographyLevel':'Geo_Type', 'StdGeographyID': 'Geo_ID','StdGeographyName':'Geo_Name','MEDVAL_CY': 'MedianHomeValue'})

# Set USA ID to 99999
data.loc[data['Geo_Type'] == 'US.WholeUSA', 'Geo_ID'] = '99999'

data.to_csv('RAW_ESRI_Msa_homevalues.csv', index=False)
