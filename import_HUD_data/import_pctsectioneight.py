import pandas as pd
from arcgis.gis import GIS
from arcgis.geocoding import geocode
import os
from arcgis.geoenrichment import enrich
import datetime
from db_layer import sql_caller
import requests as r
import sys
import re

def import_pctsectioneight():
    section_eight = pd.read_csv('Housing_Choice_Vouchers_by_Tract.csv', converters={'GEOID':str})
    section_eight = section_eight.rename(columns={'GEOID':'Geo_ID','HCV_PUBLIC_PCT':'PctSectionEight'})
    section_eight = section_eight[['Geo_ID','PctSectionEight']]
    section_eight['Geo_ID'] = section_eight['Geo_ID'].apply(lambda x: x.zfill(11))
    section_eight['PctSectionEight'] = section_eight['PctSectionEight'].fillna(0)

    return section_eight


if __name__ == "__main__":
    data = import_pctsectioneight()
    sql = sql_caller.SqlCaller(create_tables=False)
    sql.db_dump_pct_section_eight_tracts(data)





