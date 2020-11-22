import pandas as pd
import os
from sqlalchemy import create_engine
from db_layer import sql_caller




df = pd.read_excel('BLS_Zillow_Geo.xlsx')

df = df[df['Missing Metro?'] != 'Yes'].drop(columns=['State 1','State 2','Missing Metro?'])\
    .rename(columns={'ID 1':'Zillow_Id',
                    'Metro Name 1':'Zillow_MSA_Name',
                    'ID 2':'Geo_Id',
                    'Metro Name 2':'MSA_Name'})

df[['Geo_Id','Zillow_Id']] = df[['Geo_Id','Zillow_Id']].apply(lambda x: x.astype(int).astype(str))

df = df.append({'Geo_Id': '99999',
           'Zillow_Id': '102001',
           'Zillow_MSA_Name': 'United States',
            'MSA_Name':'United States'
           }, ignore_index=True)


sql = sql_caller.SqlCaller(create_tables=True)
sql.db_dump_Zillow_MSAID_Lookup(df)


