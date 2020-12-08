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
    'counties': 'https://download.bls.gov/pub/time.series/la/la.data.64.County',
    'states':'https://download.bls.gov/pub/time.series/la/la.data.2.AllStatesU'
}


# DEFINE CURRENT MONTH AND YEAR TO PULL. SET US UNEMPLOYMENT RATE
final_df = pd.DataFrame()
delimeter = '\t'
current_year = '2020'
current_month = 'M10'
US_UNEMPLOYMENT = 6.9
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

            # make sure we only look unemployment and unseasonal
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

    for i, row in df.iterrows():
        if k == 'states':
            if row['series_id'][5:7] == '72':
                df.drop(i, inplace=True)
                continue
            df.at[i, 'Geo_ID'] = row['series_id'][5:7]
            df.at[i, 'Geo_Type'] = 'States'

        elif k == 'counties':
            if row['series_id'][5:7] == '72':
                df.drop(i, inplace=True)
                continue
            df.at[i, 'Geo_ID'] = row['series_id'][5:10]
            df.at[i, 'Geo_Type'] = 'Counties'

        elif k == 'metro' or k == 'micro':
            if row['series_id'][5:7] == '72':
                df.drop(i, inplace=True)
                continue
            df.at[i, 'Geo_ID'] = row['series_id'][7:12]
            df.at[i, 'Geo_Type'] = 'Metro'

    if final_df.empty:
        final_df = df
    else:
        final_df = final_df.append(df)

MSA_to_CBSA_conversion = {
        '70750':'12620','70900':'12700','71050':'12740','71350':'13540','71500':'13620','71650':'14460','71950':'14860','72400':'15540','72700':'18180',
        '19380':'19430','73450':'25540','73750':'28300','73900':'29060','74350':'30100','74650':'30340','74950':'31700','75700':'35300','76450':'35980',
        '36860':'36837','76600':'38340','76750':'38860','39140':'39150','77200':'39300','77650':'40860','78100':'44140','78400':'45860','78500':'47240',
        '11680':'49060','79600':'49340'}

for k,v in MSA_to_CBSA_conversion.items():
    if k in list(final_df['Geo_ID']):
        final_df.loc[final_df['Geo_ID'] == k, 'Geo_ID'] = v

# Get list of MSA and State Ids to make sure there is no update we miss
geo_names_df = pd.read_excel('Geo names.xlsx',converters={'Geo_ID':str}).drop(columns=['Unnamed: 0', 'area_type_code', 'area_code', 'display_level', 'selectable', 'sort_sequence'])

common = final_df.merge(geo_names_df, on=['Geo_ID', 'Geo_ID'])
no_match_from_final_df = final_df[~final_df.Geo_ID.isin(common.Geo_ID)]
no_match_from_geo_names_df = geo_names_df[~geo_names_df.Geo_ID.isin(common.Geo_ID)]


# Make sure every MSA/State is accounted for in data pull
# no_match_from_final_df should be empty
# no_match_from_geo_names_df should have 02201, 02232, 02280
if not no_match_from_final_df.empty:
    print('!!! Mismatch in Geo names !!!', no_match_from_final_df)
if not no_match_from_geo_names_df.empty:
    print('!!! Mismatch in Geo names !!!', no_match_from_geo_names_df)

macrodata = pd.merge(geo_names_df, final_df, on='Geo_ID')
macrodata = macrodata.rename(columns={'area_text':'Geo_Name','year':'Year','period':'Month','value':'UnemploymentRate'}).drop(columns=['series_id','footnote_codes','Geo_Type_y']).rename(columns={'Geo_Type_x':'Geo_Type'})


macrodata = macrodata.append({'Geo_ID':'99999',
                              'Geo_Name':'United States',
                              'Geo_Type':'National',
                              'Year':current_year,
                              'Month':current_month,
                              'UnemploymentRate':US_UNEMPLOYMENT}, ignore_index=True)


sql = sql_caller.SqlCaller(create_tables=True)
sql.db_dump_BLS_unemployment(macrodata[['Geo_ID', 'Geo_Name', 'Year', 'Month', 'Geo_Type', 'UnemploymentRate']])

# macrodata.to_csv('misc/bls_data.csv')

