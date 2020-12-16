import pandas as pd
import os
from sqlalchemy import create_engine
from db_layer import sql_caller
import json
from datetime import date, timedelta

sql = sql_caller.SqlCaller(create_tables=True)

with open("../un_pw.json", "r") as file:
    aws_string = json.load(file)['aws_mysql']

engine = create_engine(aws_string)
today = date.today()
# today = today - timedelta(days=1)

#################################################
#   Update BACKUP_BLS_Unemployment
#################################################
BACKUP_BLS_Unemployment = pd.read_sql_query('select * from BLS_Unemployment', engine)
dates = sorted(list(pd.read_sql_query('select distinct Backup_Date from BACKUP_BLS_Unemployment', engine)['Backup_Date']))

if len(dates) == 4:
    delete_date = dates[0]
    delete_sql = """DELETE p.* FROM BACKUP_BLS_Unemployment p WHERE Backup_Date = '{}';""".format(delete_date)
    with engine.begin() as conn:  # TRANSACTION
        var = conn.execute(delete_sql)

BACKUP_BLS_Unemployment['Backup_Date'] = today
# BACKUP_BLS_Unemployment['ID'] = (BACKUP_BLS_Unemployment['Backup_Date'].astype(str) + BACKUP_BLS_Unemployment['Geo_ID']).apply(lambda x: x.replace('-',''))
sql.db_dump_BACKUP_BLS_Unemployment(BACKUP_BLS_Unemployment)



#################################################
#   Update BACKUP_ESRI_Unemployment_Adjustments
#################################################
BACKUP_ESRI_Unemployment_Adjustments = pd.read_sql_query('select * from ESRI_Unemployment_Adjustments', engine)
dates = sorted(list(pd.read_sql_query('select distinct Backup_Date from BACKUP_ESRI_Unemployment_Adjustments', engine)['Backup_Date']))

if len(dates) == 4:
    delete_date = dates[0]
    delete_sql = """DELETE p.* FROM BACKUP_ESRI_Unemployment_Adjustments p WHERE Backup_Date = '{}';""".format(delete_date)

    with engine.begin() as conn:  # TRANSACTION
        var = conn.execute(delete_sql)

BACKUP_ESRI_Unemployment_Adjustments['Backup_Date'] = today
# BACKUP_ESRI_Unemployment_Adjustments['ID'] = (BACKUP_ESRI_Unemployment_Adjustments['Backup_Date'].astype(str) + BACKUP_ESRI_Unemployment_Adjustments['Geo_ID']).apply(lambda x: x.replace('-',''))
sql.db_dump_BACKUP_ESRI_Unemployment_Adjustments(BACKUP_ESRI_Unemployment_Adjustments)



#################################################
#   Update BACKUP_ZIP_MacroData_Update
#################################################
BACKUP_ZIP_MacroData_Update = pd.read_sql_query('select * from ZIP_MacroData_Update', engine)
dates = sorted(list(pd.read_sql_query('select distinct Backup_Date from BACKUP_ZIP_MacroData_Update', engine)['Backup_Date']))

if len(dates) == 4:
    delete_date = dates[0]
    delete_sql = """DELETE p.* FROM BACKUP_ZIP_MacroData_Update p WHERE Backup_Date = '{}';""".format(delete_date)

    with engine.begin() as conn:  # TRANSACTION
        var = conn.execute(delete_sql)


BACKUP_ZIP_MacroData_Update['Backup_Date'] = today
# BACKUP_ZIP_MacroData_Update['ID'] = (BACKUP_ZIP_MacroData_Update['Backup_Date'].astype(str) + BACKUP_ZIP_MacroData_Update['ZIP'] +
#                                      BACKUP_ZIP_MacroData_Update['MSAID'] + BACKUP_ZIP_MacroData_Update['COUNTYID']).apply(lambda x: x.replace('-',''))
sql.db_dump_BACKUP_ZIP_MacroData_Update(BACKUP_ZIP_MacroData_Update)




#################################################
#   Update BACKUP_HomeValue_PriceChange
#################################################
BACKUP_HomeValue_PriceChange = pd.read_sql_query('select * from HomeValue_PriceChange', engine)
dates = sorted(list(pd.read_sql_query('select distinct Backup_Date from BACKUP_HomeValue_PriceChange', engine)['Backup_Date']))

if len(dates) == 4:
    delete_date = dates[0]
    delete_sql = """DELETE p.* FROM BACKUP_HomeValue_PriceChange p WHERE Backup_Date = '{}';""".format(delete_date)

    with engine.begin() as conn:  # TRANSACTION
        var = conn.execute(delete_sql)


BACKUP_HomeValue_PriceChange['Backup_Date'] = today
# BACKUP_HomeValue_PriceChange['ID'] = (BACKUP_HomeValue_PriceChange['Backup_Date'].astype(str) + BACKUP_HomeValue_PriceChange['Geo_ID']).apply(lambda x: str(x).replace('-',''))
sql.db_dump_BACKUP_HomeValue_PriceChange(BACKUP_HomeValue_PriceChange)

