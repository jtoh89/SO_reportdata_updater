# import pandas as pd
# from arcgis.gis import GIS
# from arcgis.geocoding import geocode
# import os
# from arcgis.geoenrichment import enrich
# import datetime
# from db_layer import sql_caller
# import requests as r
# import sys
#
#
#
# # STEP 2 - Get Zillow lookup
# sql = sql_caller.SqlCaller(create_tables=False)
# zillow_msa_lookup = sql.db_get_Zillow_MSAID_Lookup()
#
# path = os.path.dirname(os.path.abspath(__file__))
#
# # STEP 3 - Join Zillow and ESRI data. If ESRI is missing any data, use price change using zillow time series
# final_df = pd.DataFrame()
# for filename in os.listdir(path):
#     if 'Zip_' in filename:
#         write = True
#         with open(os.path.join(path, filename)) as file:
#             df = pd.read_csv(file, converters={'RegionName':str})
#
#             df = df[['RegionName',last_updated_month_esri,current_month]].rename(columns={'RegionName':'Geo_ID'})
#
#             df['PriceChange'] = 1 + (df[current_month] - df[last_updated_month_esri]) / df[last_updated_month_esri]
#
#             df = df[['Geo_ID','PriceChange',current_month]]
#
#             # df['Better_PriceChange'] = df[current_month] / df['MedianHomeValue'] * .98
#             df['Better_PriceChange'] = df[current_month] / df['MedianHomeValue']
#
#             for i, row in df.iterrows():
#                 if row['Better_PriceChange'] > 0:
#                     if row['Better_PriceChange'] < row['PriceChange']:
#                         continue
#
#                     if row['Better_PriceChange'] >= 1.1:
#                         df.at[i, 'PriceChange'] = row['Better_PriceChange'] * 1.03
#                     else:
#                         df.at[i, 'PriceChange'] = row['Better_PriceChange'] * .99
#
#             df['Geo_Type'] = 'ZIP'
#
#             df[['Geo_ID','Geo_Type','PriceChange']].rename(columns={'PriceChange':'ZIP_PriceChange'}).to_csv('zip_homevalues.csv', index=False)
#
#
#
# # STEP 4 - Store data
# sql.db_dump_HomeValue_PriceChange(final_df[['Geo_ID','Geo_Type','PriceChange']])