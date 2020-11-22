import os
import pandas as pd
from db_layer import sql_caller

###
# Download zipcode files from https://www.huduser.gov/portal/datasets/usps_crosswalk.html
##


path = os.path.dirname(os.path.abspath(__file__))

with open(path + '/ZIP_COUNTY.csv') as file:
    county_df = pd.read_csv(file, dtype=str).rename(columns={'COUNTY':'COUNTYID'})
    removed_county_df = county_df[county_df.COUNTYID.astype(str).str[:2].isin(['60','66','69','72','78'])]
    county_df = county_df[~county_df.COUNTYID.astype(str).str[:2].isin(['60','66','69','72','78'])]


with open(path + '/ZIP_CBSA.csv') as file:
    cbsa_df = pd.read_csv(file, dtype=str).rename(columns={'CBSA':'MSAID'})


cbsa_df = cbsa_df[~cbsa_df.ZIP.isin(removed_county_df.ZIP)]

common = pd.merge(cbsa_df, county_df, how='inner', left_on=['ZIP'], right_on=['ZIP'])
cbsa_missing = cbsa_df[(~cbsa_df.ZIP.isin(common.ZIP))]
county_missing = county_df[(~county_df.ZIP.isin(common.ZIP))]

if len(cbsa_missing) > 0 or len(county_missing) > 0:
    print('!!! MISMATCH IN ZIPS')

common['STATEID'] = common['COUNTYID'].str[:2]

sql = sql_caller.SqlCaller(create_tables=True)
sql.db_dump_Zipcode_to_County_MSA(common)











