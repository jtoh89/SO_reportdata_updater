import pandas as pd
import requests as r
from db_layer import sql_caller

url = 'https://download.bls.gov/pub/time.series/la/la.area'
delimeter = '\t'
series_id_len = 20
data = r.get(url)

row_list = []

count = 0
for line in data.text.splitlines():
    if count is 0:
        headers = [x.strip() for x in line.split(delimeter)]
        count += 1
    else:
        row = [x.strip() for x in line.split(delimeter, maxsplit=len(headers) - 1)]

        # make sure we only look at MSAs
        if row[0] not in ['A','D','B', 'F']:
            continue

        if len(row) > series_id_len:
            print('seriesid length is greater than {}'.format(series_id_len))
            continue

        if len(row) < len(headers):
            row.append('n/a')
            row_list.append(row)
        else:
            row_list.append(row)
        count += 1

df = pd.DataFrame(row_list, columns=headers)

for i, row in df.iterrows():
    if row['area_type_code'] == 'A':
        if row['area_code'][2:4] == '72':
            df.drop(i, inplace=True)
            continue
        df.at[i, 'Geo_ID'] = row['area_code'][2:4]
        df.at[i, 'Geo_Type'] = 'States'

    elif row['area_type_code'] == 'B':
        if row['area_code'][2:4] == '72':
            df.drop(i, inplace=True)
            continue
        df.at[i, 'Geo_ID'] = row['area_code'][4:9]
        df.at[i, 'Geo_Type'] = 'Metro'

    elif row['area_type_code'] == 'D':
        if row['area_code'][2:4] == '72':
            df.drop(i, inplace=True)
            continue
        df.at[i, 'Geo_ID'] = row['area_code'][4:9]
        df.at[i, 'Geo_Type'] = 'Micro'

    elif row['area_type_code'] == 'F':
        if row['area_code'][2:4] == '72':
            df.drop(i, inplace=True)
            continue
        df.at[i, 'Geo_ID'] = row['area_code'][2:7]
        df.at[i, 'Geo_Type'] = 'County'


sql = sql_caller.SqlCaller(create_tables=True)
sql.db_dump_BLS_Geo_names(df[['Geo_ID', 'area_code', 'Geo_Type', 'area_text']].rename(columns={'area_text':'Geo_Name'}))

df.to_excel('Geo names.xlsx')