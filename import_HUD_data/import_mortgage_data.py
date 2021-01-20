import pandas as pd
from arcgis.gis import GIS
from arcgis.geocoding import geocode
import os
from arcgis.geoenrichment import enrich
import datetime
from db_layer import sql_caller
import requests as r
from import_HUD_data import import_pctsectioneight
import sys
import re

path = os.path.dirname(os.path.abspath(__file__))



pctsectioneight = import_pctsectioneight.import_pctsectioneight()

for filename in os.listdir(path):
    if 'Active_Foreclosure' in filename:
        with open(os.path.join(path, filename)) as file:
            foreclosure_df = pd.read_csv(file, converters={'GEOID':str,'STATEFP':str,'COUNTYFP':str})
            foreclosure_df['GEOID'] = foreclosure_df['GEOID'].apply(lambda x: x.zfill(11))
            foreclosure_df['STATEFP'] = foreclosure_df['STATEFP'].apply(lambda x: x.zfill(2))
            foreclosure_df['COUNTYFP'] = foreclosure_df['COUNTYFP'].apply(lambda x: x.zfill(3))

            foreclosure_df['County_UID'] = foreclosure_df['STATEFP'] + foreclosure_df['COUNTYFP']

            foreclosure_cols = ['Geo_ID','County_UID']
            foreclosure_date_dict = {}

            foreclosure_df = foreclosure_df.rename(columns={'GEOID': 'Geo_ID'})

            for col in foreclosure_df.columns.values:
                if 'FORECLOSURES' in col:
                    foreclosure_cols.append(col)
                    year = re.findall("(\d\d\d\d)", col)
                    month = col[-2:]

                    if len(year) != 1:
                        print('!!! Something wrong with Year parse !!!')
                        sys.exit()

                    foreclosure_date_dict[col] = col[-2:] + '-01-' + str(year[0])

            foreclosure_df = foreclosure_df[foreclosure_cols]

            foreclosure_df = foreclosure_df.melt(id_vars=['Geo_ID','County_UID'], var_name='Date', value_name='Foreclosures')

            foreclosure_df['Foreclosures'] = foreclosure_df['Foreclosures'].fillna(0)
            foreclosure_df['Foreclosures'][foreclosure_df['Foreclosures'] < 0] = 0
            foreclosure_df['Date'] = foreclosure_df['Date'].replace(foreclosure_date_dict)

            foreclosure_county = foreclosure_df[['County_UID','Date','Foreclosures']]
            foreclosure_county = foreclosure_county.groupby(['County_UID','Date'], as_index=False).sum()

    if '90_Day_Defaults' in filename:
        with open(os.path.join(path, filename)) as file:
            ninetydaysdefault_df = pd.read_csv(file, converters={'GEOID':str,'STATEFP':str,'COUNTYFP':str})
            ninetydaysdefault_df['GEOID'] = ninetydaysdefault_df['GEOID'].apply(lambda x: x.zfill(11))
            ninetydaysdefault_df['STATEFP'] = ninetydaysdefault_df['STATEFP'].apply(lambda x: x.zfill(2))
            ninetydaysdefault_df['COUNTYFP'] = ninetydaysdefault_df['COUNTYFP'].apply(lambda x: x.zfill(3))

            ninetydaysdefault_df['County_UID'] = ninetydaysdefault_df['STATEFP'] + ninetydaysdefault_df['COUNTYFP']

            ninetydaysdefault_cols = ['Geo_ID','County_UID']
            ninetydaysdefault_date_dict = {}

            ninetydaysdefault_df = ninetydaysdefault_df.rename(columns={'GEOID': 'Geo_ID'})

            for col in ninetydaysdefault_df.columns.values:
                if '90_DAY' in col:
                    ninetydaysdefault_cols.append(col)
                    year = re.findall("(\d\d\d\d)", col)
                    month = col[-2:]

                    if len(year) != 1:
                        print('!!! Something wrong with Year parse !!!')
                        sys.exit()

                    ninetydaysdefault_date_dict[col] = col[-2:] + '-01-' + str(year[0])

            ninetydaysdefault_df = ninetydaysdefault_df[ninetydaysdefault_cols]

            ninetydaysdefault_df = ninetydaysdefault_df.melt(id_vars=['Geo_ID','County_UID'], var_name='Date', value_name='NinetyDaysDefault')
            ninetydaysdefault_df['NinetyDaysDefault'] = ninetydaysdefault_df['NinetyDaysDefault'].fillna(0)
            ninetydaysdefault_df['NinetyDaysDefault'][ninetydaysdefault_df['NinetyDaysDefault'] < 0] = 0
            ninetydaysdefault_df['Date'] = ninetydaysdefault_df['Date'].replace(ninetydaysdefault_date_dict)


            ninetydaysdefault_county = ninetydaysdefault_df[['County_UID','Date','NinetyDaysDefault']]
            ninetydaysdefault_county = ninetydaysdefault_county.groupby(['County_UID','Date'], as_index=False).sum()

final_dates = set(foreclosure_date_dict.values()).intersection(ninetydaysdefault_date_dict.values())

foreclosure_df = foreclosure_df[foreclosure_df['Date'].isin(final_dates)]
ninetydaysdefault_df = ninetydaysdefault_df[ninetydaysdefault_df['Date'].isin(final_dates)]

final_df = pd.merge(foreclosure_df, ninetydaysdefault_df[['Date','Geo_ID','NinetyDaysDefault']], how='inner', left_on=['Geo_ID','Date'], right_on=['Geo_ID','Date'])
ninety_missing = ninetydaysdefault_df[(~ninetydaysdefault_df.Geo_ID.isin(final_df.Geo_ID))]
foreclosure_missing = foreclosure_df[(~foreclosure_df.Geo_ID.isin(final_df.Geo_ID))]

if len(foreclosure_missing) > 0 or len(ninety_missing) > 0:
    print('!!! MISMATCH !!!! in foreclosure and 90 day default data')
    sys.exit()




final_df = pd.merge(final_df, pctsectioneight[['Geo_ID', 'PctOfRentersSecEight']], how='left', left_on=['Geo_ID'], right_on=['Geo_ID'])
final_df['PctOfRentersSecEight'] = final_df['PctOfRentersSecEight'].fillna(0)



sql = sql_caller.SqlCaller(create_tables=True)
sql.db_dump_HUD_CensusTractsData(final_df[['Geo_ID','Date','NinetyDaysDefault','Foreclosures','PctOfRentersSecEight']])





