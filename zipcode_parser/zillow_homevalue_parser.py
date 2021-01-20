import pandas as pd
from arcgis.gis import GIS
from arcgis.geocoding import geocode
import os
from arcgis.geoenrichment import enrich
import datetime
from db_layer import sql_caller
import requests as r
import sys


path = os.path.dirname(os.path.abspath(__file__))

# STEP 1 - Get all of esri home values
esri_counties_df = pd.read_csv('../import_esri_homevalues/RAW_ESRI_Counties_HomeValues.csv')
esri_counties_df['Geo_ID'] = esri_counties_df['Geo_ID'].apply(lambda x: str(x).zfill(5))

esri_msa_df = pd.read_csv('../import_esri_homevalues/RAW_ESRI_Msa_homevalues.csv')
state_df = esri_msa_df[esri_msa_df['Geo_Type'] == 'US.States']
state_df['Geo_ID'] = state_df['Geo_ID'].apply(lambda x: str(x).zfill(2))
esri_msa_df = esri_msa_df[esri_msa_df['Geo_Type'] != 'US.States']
esri_msa_df['Geo_ID'] = esri_msa_df['Geo_ID'].apply(lambda x: str(x).zfill(5))
esri_msa_df = state_df.append(esri_msa_df)

esri_zip_df = pd.read_csv('../import_esri_homevalues/RAW_ESRI_Zip_HomeValues.csv')
esri_zip_df = esri_zip_df[esri_zip_df['MedianHomeValue'] != 0]
esri_zip_df['Geo_ID'] = esri_zip_df['Geo_ID'].apply(lambda x: str(x).zfill(5))






# STEP 2 - Get Zillow lookup
sql = sql_caller.SqlCaller(create_tables=False)
zillow_msa_lookup = sql.db_get_Zillow_MSAID_Lookup()

path = os.path.dirname(os.path.abspath(__file__))

last_updated_month_esri = '2020-07-31'

# STEP 3 - Join Zillow and ESRI data. If ESRI is missing any data, use price change using zillow time series
final_df = pd.DataFrame()
for filename in os.listdir(path):
    write = False
    if 'Metro_' in filename:
        write = True
        with open(os.path.join(path, filename)) as file:
            df = pd.read_csv(file)
            current_month = df.columns[-1]

            df['RegionID'] = df['RegionID'].apply(lambda x: str(x))
            df = df[['RegionID',last_updated_month_esri,current_month]]

            df = pd.merge(df, zillow_msa_lookup, how='left', left_on=['RegionID'], right_on=['Zillow_Id'])

            df['PriceChange'] = 1 + (df[current_month] - df[last_updated_month_esri]) / df[last_updated_month_esri]

            df = df[['Geo_ID','PriceChange',current_month]]
            df = pd.merge(df, esri_msa_df[['Geo_ID','MedianHomeValue']], how='left', left_on=['Geo_ID'], right_on=['Geo_ID'])
            df['Better_PriceChange'] = df[current_month] / df['MedianHomeValue'] * .98

            for i, row in df.iterrows():
                if row['Better_PriceChange'] > 0:
                    df.at[i, 'PriceChange'] = row['Better_PriceChange']

            df['Geo_Type'] = 'MSA'
            df.loc[df['Geo_ID'] == '99999', 'Geo_Type'] = 'National'

            df[['Geo_ID','Geo_Type','PriceChange']].rename(columns={'PriceChange':'MSA_PriceChange'}).to_csv('msa_homevalues.csv', index=False)



    if 'County_' in filename:
        write = True
        with open(os.path.join(path, filename)) as file:
            df = pd.read_csv(file)
            current_month = df.columns[-1]

            df = df[['State', 'StateCodeFIPS', 'MunicipalCodeFIPS', 'RegionName', current_month, last_updated_month_esri]]

            df['Geo_ID'] = df['StateCodeFIPS'].apply(lambda x: str(x).zfill(2)) + df['MunicipalCodeFIPS'].apply(lambda x: str(x).zfill(3))

            df['PriceChange'] = 1 + (df[current_month] - df[last_updated_month_esri]) / df[last_updated_month_esri]

            df = df[['Geo_ID','PriceChange',current_month]]

            df = pd.merge(df, esri_counties_df[['Geo_ID','MedianHomeValue']], how='left', left_on=['Geo_ID'], right_on=['Geo_ID'])
            df['Better_PriceChange'] = df[current_month] / df['MedianHomeValue'] * .98

            for i, row in df.iterrows():
                if row['Better_PriceChange'] > 0:
                    df.at[i, 'PriceChange'] = row['Better_PriceChange']


            df['Geo_Type'] = 'Counties'

            df[['Geo_ID','Geo_Type','PriceChange']].rename(columns={'PriceChange':'COUNTY_PriceChange'}).to_csv('county_homevalues.csv', index=False)


    if 'Zip_' in filename:
        write = True
        with open(os.path.join(path, filename)) as file:
            df = pd.read_csv(file, converters={'RegionName':str})
            current_month = df.columns[-1]

            df = df[['RegionName',last_updated_month_esri,current_month]].rename(columns={'RegionName':'Geo_ID'})

            df['PriceChange'] = 1 + (df[current_month] - df[last_updated_month_esri]) / df[last_updated_month_esri]

            df = df[['Geo_ID','PriceChange',current_month]]

            df = pd.merge(df, esri_zip_df[['Geo_ID','MedianHomeValue']], how='left', left_on=['Geo_ID'], right_on=['Geo_ID'])
            # df['Better_PriceChange'] = df[current_month] / df['MedianHomeValue'] * .98
            df['Better_PriceChange'] = df[current_month] / df['MedianHomeValue']

            for i, row in df.iterrows():
                if row['Better_PriceChange'] > 0:
                    if row['Better_PriceChange'] < row['PriceChange']:
                        continue

                    if row['Better_PriceChange'] >= 1.1:
                        df.at[i, 'PriceChange'] = row['Better_PriceChange'] * 1.03
                    else:
                        df.at[i, 'PriceChange'] = row['Better_PriceChange'] * .99

            df['Geo_Type'] = 'ZIP'

            df[['Geo_ID','Geo_Type','PriceChange']].rename(columns={'PriceChange':'ZIP_PriceChange'}).to_csv('zip_homevalues.csv', index=False)

    if write:
        if final_df.empty:
            final_df = df
        else:
            final_df = final_df.append(df)


# STEP 4 - Store data
sql.db_dump_HomeValue_PriceChange(final_df[['Geo_ID','Geo_Type','PriceChange']])