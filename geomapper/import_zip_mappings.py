import os
import pandas as pd
from db_layer import sql_caller

###
# This script will Zipcode to County to MSA Ids
# Download zipcode files from https://www.huduser.gov/portal/datasets/usps_crosswalk.html
##


path = os.path.dirname(os.path.abspath(__file__))

with open(path + '/ZIP_COUNTY.csv') as file:
    county_df = pd.read_csv(file, dtype=str).rename(columns={'COUNTY':'COUNTYID'})
    puertorico_df = county_df[county_df.COUNTYID.astype(str).str[:2].isin(['60', '66', '69', '72', '78'])]
    county_df = county_df[~county_df.COUNTYID.astype(str).str[:2].isin(['60','66','69','72','78'])]

    non_PR_count = len(county_df)

with open(path + '/ZIP_CBSA.csv') as file:
    cbsa_df = pd.read_csv(file, dtype=str).rename(columns={'CBSA':'MSAID'})

# Drop all cbsas where Zipcode is partially out of an urban area
zip_notin_cbsa = cbsa_df[cbsa_df['MSAID'] == '99999']['ZIP']
dupe_zip = []
for zip in zip_notin_cbsa:
    if len(cbsa_df[cbsa_df['ZIP'] == zip]) > 1:
        dupe_zip.append(zip)
    else:
        continue

for i, row in cbsa_df.iterrows():
    if row['ZIP'] in dupe_zip and row['MSAID'] == '99999':
        cbsa_df.drop(i, inplace=True)

# Drop all cbsas where Zipcode is in puertorico
cbsa_df = cbsa_df[~cbsa_df.ZIP.isin(puertorico_df.ZIP)]


sql = sql_caller.SqlCaller(create_tables=False)
msa_countystate = sql.db_get_MSA_to_CountyState()
county_test = pd.merge(county_df, msa_countystate[['MSAID','COUNTYID']], how='left', left_on=['COUNTYID'], right_on=['COUNTYID'])


# common = pd.merge(cbsa_df, county_df, how='inner', left_on=['ZIP'], right_on=['ZIP'])
common = pd.merge(county_test, cbsa_df, how='left', left_on=['ZIP','MSAID'], right_on=['ZIP','MSAID'])
cbsa_missing = cbsa_df[(~cbsa_df.ZIP.isin(common.ZIP))]
county_missing = county_df[(~county_df.ZIP.isin(common.ZIP))]

if len(cbsa_missing) > 0 or len(county_missing) > 0:
    print('!!! MISMATCH IN ZIPS')

common['STATEID'] = common['COUNTYID'].str[:2]
common['MSAID'] = common['MSAID'].fillna('')

sql = sql_caller.SqlCaller(create_tables=True)
sql.db_dump_Zipcode_to_CountyMSAState(common)



