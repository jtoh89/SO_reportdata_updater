import pandas as pd
from arcgis.gis import GIS
from arcgis.geocoding import geocode
import os
from arcgis.geoenrichment import enrich
import datetime
from db_layer import sql_caller
import requests as r





path = os.path.dirname(os.path.abspath(__file__))


#   STEP 1
#   Get home values for each zipcode, msa, and county


for filename in os.listdir(path):
    if 'zip_homevalues' in filename:
        zip_df = pd.read_csv('zip_homevalues.csv')
        zip_df = zip_df.drop(columns=['Unnamed: 0'])
        zip_df['ZIP'] = zip_df['Geo_ID'].apply(lambda x: str(x).zfill(5))
    elif 'msa_homevalues' in filename:
        msa_df = pd.read_csv('msa_homevalues.csv')
        msa_df = msa_df.drop(columns=['Unnamed: 0']).fillna(0)
        msa_df['MSAID'] = msa_df['Geo_ID'].apply(lambda x: str(int(x)).zfill(5))
        us_multiplier = msa_df[msa_df.MSAID == '99999']['MSA_PriceChange'][0]
        msa_df = msa_df[msa_df.MSAID != '99999']
    elif 'county_homevalues' in filename:
        county_df = pd.read_csv('county_homevalues.csv')
        county_df = county_df.drop(columns=['Unnamed: 0'])
        county_df['COUNTYID'] = county_df['Geo_ID'].apply(lambda x: str(x).zfill(5))


#   STEP 2
#   Get all USPS zipcodes mapped to the County and MSA (CBSA) IDs

sql = sql_caller.SqlCaller(create_tables=True)
zip_msa_county_lookup = sql.db_get_GeoMapping_Zipcode_to_CountyMSAState()


#   STEP 3
#   Join Zipcode mapping to msa, county, and zipcode home values. Use left join to keep all zipcodes

final_df = pd.merge(zip_msa_county_lookup, zip_df.drop(columns=['Geo_ID','Geo_Type']), how='left', left_on=['ZIP'], right_on=['ZIP'])
final_df = pd.merge(final_df, msa_df.drop(columns=['Geo_ID','Geo_Type']), how='left', left_on=['MSAID'], right_on=['MSAID'])
final_df = pd.merge(final_df, county_df.drop(columns=['Geo_ID','Geo_Type']), how='left', left_on=['COUNTYID'], right_on=['COUNTYID'])
final_df['USA_PriceChange'] = us_multiplier


#   STEP 4
#   Get unemployment for county, msa, and usa. Join on IDs

unemployment = sql.db_get_BLS_all_unemployment()

county_unemployment_lookup = unemployment[unemployment['Geo_Type'] == 'Counties'].rename(columns={'UnemploymentRate':'COUNTY_UnemploymentRate'})
msa_unemployment_lookup = unemployment[unemployment['Geo_Type'].isin(['Metro','Micro'])].rename(columns={'UnemploymentRate':'MSA_UnemploymentRate'})
national_unemployment_lookup = unemployment[unemployment['Geo_Type'] == 'National'].rename(columns={'UnemploymentRate':'USA_UnemploymentRate'})

final_df = pd.merge(final_df, county_unemployment_lookup, how='left', left_on=['COUNTYID'], right_on=['Geo_ID']).drop(columns=['Geo_ID','Geo_Type'])
final_df = pd.merge(final_df, msa_unemployment_lookup, how='left', left_on=['MSAID'], right_on=['Geo_ID']).drop(columns=['Geo_ID','Geo_Type'])
final_df['USA_UnemploymentRate'] = national_unemployment_lookup['USA_UnemploymentRate'].iloc[0]


#   STEP 5
#   Get unemployment adjustment values for zipcode and census tract
# unemployment_multiplier = sql.db_get_ESRI_unemployment_data()
unemployment_multiplier = sql.db_get_ESRI_unemployment_adjustment_data()
unemployment_msa_multiplier = unemployment_multiplier[unemployment_multiplier['Geo_Type'] == 'US.CBSA'].rename(columns={'Unemployment_Adjustment':'MSA_Unemployment_Adjustment'})
unemployment_county_multiplier = unemployment_multiplier[unemployment_multiplier['Geo_Type'] == 'US.Counties'].rename(columns={'Unemployment_Adjustment':'COUNTY_Unemployment_Adjustment'})
unemployment_state_multiplier = unemployment_multiplier[unemployment_multiplier['Geo_Type'] == 'US.States'].rename(columns={'Unemployment_Adjustment':'STATE_Unemployment_Adjustment'})

final_df = pd.merge(final_df, unemployment_msa_multiplier, how='left', left_on=['MSAID'], right_on=['Geo_ID']).drop(columns=['Geo_ID','Geo_Type'])
final_df = pd.merge(final_df, unemployment_county_multiplier, how='left', left_on=['COUNTYID'], right_on=['Geo_ID']).drop(columns=['Geo_ID','Geo_Type'])
final_df = pd.merge(final_df, unemployment_state_multiplier, how='left', left_on=['STATEID'], right_on=['Geo_ID']).drop(columns=['Geo_ID','Geo_Type'])


#   STEP 6
#   Dump data into db
sql.db_dump_ZIP_MacroData_Update(final_df)


