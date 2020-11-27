import pandas as pd
from arcgis.gis import GIS
from arcgis.geocoding import geocode
import os
from arcgis.geoenrichment import enrich
import datetime
from db_layer import sql_caller
import requests as r



## This script will store MSA and County Home Value Price Change


sql = sql_caller.SqlCaller(create_tables=True)
zillow_msa_lookup = sql.db_get_Zillow_MSAID_Lookup()

path = os.path.dirname(os.path.abspath(__file__))

last_updated_month_esri = '2020-07-31'

for filename in os.listdir(path):
    if 'Metro_' in filename:
        with open(os.path.join(path, filename)) as file:
            df = pd.read_csv(file)
            current_month = df.columns[-1]

            df['RegionID'] = df['RegionID'].apply(lambda x: str(x))
            df = df[['RegionID',last_updated_month_esri,current_month]]

            common = pd.merge(df, zillow_msa_lookup, how='left', left_on=['RegionID'], right_on=['Zillow_Id'])

            common['MSA_PriceChange'] = (common[current_month] - common[last_updated_month_esri]) / common[last_updated_month_esri]

            common = common[['Geo_ID','MSA_PriceChange']].rename(columns={'Geo_ID':'MSAID'})

            common.to_csv('msa_homevalues.csv')

            sql.db_dump_HomeValue_PriceChange_MSA(common)

    if 'County_' in filename:
        with open(os.path.join(path, filename)) as file:
            df = pd.read_csv(file)
            current_month = df.columns[-1]

            df = df[['State', 'StateCodeFIPS', 'MunicipalCodeFIPS', 'RegionName', current_month, last_updated_month_esri]]

            df['COUNTYID'] = df['StateCodeFIPS'].apply(lambda x: str(x).zfill(2)) + df['MunicipalCodeFIPS'].apply(lambda x: str(x).zfill(3))

            df['COUNTY_PriceChange'] = (df[current_month] - df[last_updated_month_esri]) / df[last_updated_month_esri]

            df = df[['COUNTYID','COUNTY_PriceChange']]

            df.to_csv('county_homevalues.csv')

            sql.db_dump_HomeValue_PriceChange_County(df)


    if 'Zip_' in filename:
        with open(os.path.join(path, filename)) as file:
            df = pd.read_csv(file)
            current_month = df.columns[-1]

            df = df[['RegionName',last_updated_month_esri,current_month]].rename(columns={'RegionName':'ZIP'})

            df['ZIP_PriceChange'] = (df[current_month] - df[last_updated_month_esri]) / df[last_updated_month_esri]

            df = df[['ZIP','ZIP_PriceChange']]

            df.to_csv('zip_homevalues.csv')