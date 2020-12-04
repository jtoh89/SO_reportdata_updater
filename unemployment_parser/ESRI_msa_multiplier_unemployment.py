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
        'UNEMPRT_CY': 'Unemployment Rate'
}

# gis = GIS('https://www.arcgis.com', 'arcgis_python', 'P@ssword123')


##   CALL GEOENRICH API
data = enrich(study_areas=[{"sourceCountry":"US","layer":"US.States","ids":['01','02','04','05','06','08','09','10','11','12','13','15','16',
                                                                            '17','18','19','20','21','22','23','24','25','26','27','28','29',
                                                                            '30','31','32','33','34','35','36','37','38','39','40','41','42',
                                                                            '44','45','46','47','48','49','50','51','53','54','55','56']}],
              analysis_variables=list(dict.keys()),
              comparison_levels=['US.CBSA','US.WholeUSA'],
              return_geometry=False)

# data.to_excel('RAW_esri_unemployment.xlsx')
data = pd.read_excel('RAW_esri_unemployment.xlsx')

data = data.drop(columns=['ID', 'apportionmentConfidence', 'OBJECTID','aggregationMethod','populationToPolygonSizeRating', 'HasData', 'sourceCountry'])
data = data.rename(columns={'StdGeographyLevel':'Geo_Type', 'StdGeographyID': 'Geo_ID','StdGeographyName':'Geo_Name','UNEMPRT_CY': 'UnemploymentRate'})


macro_df = data

# Set USA ID to 99999
macro_df.loc[macro_df['Geo_Type'] == 'US.WholeUSA', 'Geo_ID'] = '99999'


# Get BLS Data and join on ESRI data
sql = sql_caller.SqlCaller(create_tables=True)
bls_unemployment_data = sql.db_get_BLS_msa_unemployment()
bls_unemployment_data['UnemploymentRate'] = bls_unemployment_data['UnemploymentRate'].astype(float)
match = pd.merge(bls_unemployment_data, macro_df, on='Geo_ID').rename(columns={'UnemploymentRate_x': 'UnemploymentRate_BLS', 'UnemploymentRate_y': 'UnemploymentRate_ESRI'})


# Make sure there are no new missing ESRI IDs that we have no accounted for
arcgisids_not_in_BLS = macro_df[(~macro_df.Geo_ID.isin(match.Geo_ID))]
for i, row in arcgisids_not_in_BLS.iterrows():
    if row['Geo_ID'] not in ['12300','24180','27530','33380','34350','39100','40530','42500','46420']:
        arcgisids_not_in_BLS.to_excel('arcgis_missing_mlsids.xlsx')
        sys.exit()


# Set multiplier
match['Unemployment_multiplier'] = match['UnemploymentRate_BLS'] / match['UnemploymentRate_ESRI']


sql.db_dump_ESRI_Unemployment_Multiplier(match[['Geo_ID', 'Geo_Name', 'Geo_Type', 'UnemploymentRate_ESRI', 'UnemploymentRate_BLS', 'Unemployment_multiplier']])



