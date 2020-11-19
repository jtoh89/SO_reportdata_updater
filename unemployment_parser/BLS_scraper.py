import pandas as pd
import requests as r
from db_layer import sql_caller
import sys

############################################
######## METRO and STATE UNEMPLOYMENT
############################################

urls = {
    'micro':'https://download.bls.gov/pub/time.series/la/la.data.62.Micro',
        'metro':'https://download.bls.gov/pub/time.series/la/la.data.60.Metro',
        'states':'https://download.bls.gov/pub/time.series/la/la.data.2.AllStatesU'}


# DEFINE CURRENT MONTH AND YEAR TO PULL. SET US UNEMPLOYMENT RATE
final_df = pd.DataFrame()
delimeter = '\t'
current_year = '2020'
current_month = 'M09'
US_UNEMPLOYMENT = 7.9
#https://fred.stlouisfed.org/series/UNRATE

# Parse Data
for k,v in urls.items():
    data = r.get(v)

    row_list = []

    count = 0
    for line in data.text.splitlines():
        if count is 0:
            headers = [x.strip() for x in line.split(delimeter)]
            count += 1
        else:
            row = [x.strip() for x in line.split(delimeter, maxsplit=len(headers) - 1)]

            # make sure we only look at MSAs
            if row[1] != current_year or row[2] != current_month or row[0][-2:] != '03' or row[0][2] != 'U':
                continue

            if len(row) != len(headers):
                print('There is a mismatch in columns and values')
                print(row)
                row.append('n/a')
                row_list.append(row)
            else:
                row_list.append(row)
            count += 1

    df = pd.DataFrame(row_list, columns=headers)

    if k == 'states':
        df['Geo_ID'] = df['series_id'].str[5:7]
    else:
        df['Geo_ID'] = df['series_id'].str[7:12]

    if final_df.empty:
        final_df = df
    else:
        final_df = final_df.append(df)

# Get list of MSA and State Ids to make sure there is no update we miss
geo_names_df = pd.read_excel('Geo names.xlsx')
geo_names_df['Geo_ID'] = geo_names_df['Geo_ID'].astype(str).str.zfill(2)
geo_names_df = geo_names_df.drop(columns=['Unnamed: 0', 'area_type_code', 'area_code', 'display_level', 'selectable', 'sort_sequence'])


# Make sure every MSA/State is accounted for in data pull
if len(geo_names_df) != len(final_df):
    print('!!! Something Wrong - there is a mismatch in records')
    sys.exit()

macrodata = pd.merge(geo_names_df, final_df, on='Geo_ID')
macrodata = macrodata.rename(columns={'area_text':'Geo_Name','year':'Year','period':'Month','value':'UnemploymentRate'}).drop(columns=['series_id','footnote_codes'])


macrodata = macrodata.append({'Geo_ID':'999',
                              'Geo_Name':'United States',
                              'Year':current_year,
                              'Month':current_month,
                              'UnemploymentRate':US_UNEMPLOYMENT}, ignore_index=True)


sql = sql_caller.SqlCaller(create_tables=False)
sql.db_dump_BLS_Macro_data(macrodata[['Geo_ID', 'Geo_Name','Year', 'Month', 'UnemploymentRate']])

# macrodata.to_csv('misc/bls_data.csv')

