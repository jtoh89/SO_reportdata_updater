import pandas as pd
from arcgis.gis import GIS
from arcgis.geocoding import geocode
import os
from arcgis.geoenrichment import enrich
import datetime
from db_layer import sql_caller
import requests as r





path = os.path.dirname(os.path.abspath(__file__))


for filename in os.listdir(path):
    if 'zip_homevalues' in filename:
        zip_df = pd.read_csv('zip_homevalues.csv')
        zip_df = zip_df.drop(columns=['Unnamed: 0'])
        zip_df['ZIP'] = zip_df['ZIP'].apply(lambda x: str(x).zfill(5))
    elif 'msa_homevalues' in filename:
        msa_df = pd.read_csv('msa_homevalues.csv')
        msa_df = msa_df.drop(columns=['Unnamed: 0']).fillna(0)
        msa_df['MSAID'] = msa_df['MSAID'].apply(lambda x: str(int(x)).zfill(5))
    elif 'county_homevalues' in filename:
        county_df = pd.read_csv('county_homevalues.csv')
        county_df = county_df.drop(columns=['Unnamed: 0'])
        county_df['COUNTYID'] = county_df['COUNTYID'].apply(lambda x: str(x).zfill(5))

# with open(os.path.normpath(os.getcwd() + os.sep + os.pardir) + '/geomapper/ZIP_CBSA.csv', "r") as file:
#     zip_msa_df = pd.read_csv(file)
#     zip_msa_df['ZIP']= zip_msa_df['ZIP'].apply(lambda x: str(x).zfill(5))
#     zip_msa_df['CBSA'] = zip_msa_df['CBSA'].apply(lambda x: str(x).zfill(5))
#
# with open(os.path.normpath(os.getcwd() + os.sep + os.pardir) + '/geomapper/ZIP_COUNTY.csv', "r") as file:
#     zip_county_df = pd.read_csv(file)
#     zip_county_df['ZIP'] = zip_county_df['ZIP'].apply(lambda x: str(x).zfill(5))
#     zip_county_df['COUNTY'] = zip_county_df['COUNTY'].apply(lambda x: str(x).zfill(5))


sql = sql_caller.SqlCaller()
zip_msa_county_lookup = sql.db_get_Zipcode_to_County_MSA()

final_df = pd.merge(zip_msa_county_lookup, zip_df, how='left', left_on=['ZIP'], right_on=['ZIP']).rename(columns={'PriceChange':'ZIP_PriceChange'})
final_df = pd.merge(final_df, msa_df, how='left', left_on=['MSAID'], right_on=['MSAID']).rename(columns={'PriceChange':'MSA_PriceChange'})
final_df = pd.merge(final_df, county_df, how='left', left_on=['COUNTYID'], right_on=['COUNTYID']).rename(columns={'PriceChange':'COUNTY_PriceChange'})


print(final_df)

store lookup

