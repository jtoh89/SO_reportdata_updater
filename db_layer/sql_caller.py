from sqlalchemy import create_engine
from db_layer import models
import pandas as pd
import json

class SqlCaller():
    """
    SqlCaller() --> This module is meant to dump various data into a database.
    Must instantiate SqlCaller() with census api key and db connection string

    Parameters:
    engine_string: define db connection here
    api_key: define api_key for census data. You can go to www.census.gov.
    """

    def __init__(self, create_tables=False):
        connectagain = False
        try:
            with open("./un_pw.json", "r") as file:
                mysql_engine = json.load(file)['aws_mysql']
        except:
            connectagain = True

        if connectagain:
            with open("../un_pw.json", "r") as file:
                mysql_engine = json.load(file)['aws_mysql']


        engine_string = mysql_engine
        self.engine = create_engine(engine_string)

        if create_tables == True:
            print("Creating tables")
            models.InitiateDeclaratives.create_tables(engine_string)



    def db_dump_BLS_unemployment(self, df):
        df.to_sql("BLS_Unemployment", if_exists='replace', con=self.engine, index=False)

    def db_get_BLS_msa_unemployment(self):
        df = pd.read_sql_query("select Geo_ID, UnemploymentRate from BLS_Unemployment where Geo_Type != 'County'", self.engine)

        return df

    def db_get_BLS_all_unemployment(self):
        df = pd.read_sql_query("select Geo_ID, UnemploymentRate, Geo_Type from BLS_Unemployment", self.engine)

        return df



    def db_dump_ESRI_Unemployment_Multiplier(self, df):
            df.to_sql("ESRI_Unemployment_Multiplier", if_exists='replace', con=self.engine, index=False)

    def db_get_ESRI_unemployment_data(self):
        df = pd.read_sql_query("""select Geo_ID, Geo_Type, Unemployment_multiplier from ESRI_Unemployment_Multiplier 
                                    where Geo_Type in ('US.CBSA','US.States')""", self.engine)
        return df



    def db_dump_Zipcode_to_CountyMSAState(self, df):
        df.to_sql("Zipcode_to_CountyMSAState", if_exists='replace', con=self.engine, index=False)


    def db_get_Zipcode_to_CountyMSAState(self):
        df = pd.read_sql_query("select * from Zipcode_to_CountyMSAState", self.engine)

        return df



    def db_dump_ZIP_MacroData_Update(self, df):
        df.to_sql("ZIP_MacroData_Update", if_exists='replace', con=self.engine, index=False)

    def db_dump_HomeValue_PriceChange_MSA(self, df):
        df.to_sql("HomeValue_PriceChange_MSA", if_exists='replace', con=self.engine, index=False)

    def db_dump_HomeValue_PriceChange_County(self, df):
        df.to_sql("HomeValue_PriceChange_County", if_exists='replace', con=self.engine, index=False)



    def db_dump_MSA_to_CountyState(self, df):
        df.to_sql("MSA_to_CountyState", if_exists='append', con=self.engine, index=False)

    def db_get_MSA_to_CountyState(self):
        msa_ids = pd.read_sql_query("""select * from MSA_to_CountyState""", self.engine)
        return msa_ids



    def db_dump_Zillow_MSAID_Lookup(self, df):
        df.to_sql("Zillow_MSAID_Lookup", if_exists='replace', con=self.engine, index=False)

    def db_get_Zillow_MSAID_Lookup(self):
        msa_ids = pd.read_sql_query("""select Geo_ID, Zillow_Id from Zillow_MSAID_Lookup""", self.engine)
        return msa_ids



    def db_dump_BLS_Geo_Info(self, df):
        df.to_sql("BLS_Geo_Info", if_exists='replace', con=self.engine, index=False)




