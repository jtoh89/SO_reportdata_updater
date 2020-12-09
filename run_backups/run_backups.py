import pandas as pd
import os
from sqlalchemy import create_engine
from db_layer import sql_caller


sql = sql_caller.SqlCaller(create_tables=True)






data_adjustment = pd.read_sql_query(""" select *
                                        from ZIP_MacroData_Update
                                        where ZIP = '{}' and COUNTYID = '{}' and MSAID = '{}'
                                        """.format(zipcode,countyid,msaid), create_engine(aws_string))




