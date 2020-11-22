from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Table, MetaData, create_engine, Column, Integer, String, Float, BigInteger, Date
import datetime

Base = declarative_base()



#   Macro Data
class BLS_Geo_Info(Base):
    __tablename__ = "BLS_Geo_Info"
    Geo_ID = Column(String(10), unique=False, primary_key=True)
    area_code = Column(String(100), unique=False)
    Geo_Name = Column(String(100), unique=False)
    Geo_Type = Column(String(50), unique=False)


#   Macro Data
class BLS_Unemployment(Base):
    __tablename__ = "BLS_Unemployment"

    Geo_ID = Column(String(10), unique=False, primary_key=True)
    Geo_Name = Column(String(50), unique=False)
    Geo_Type = Column(String(50), unique=False)
    Year = Column(Integer, unique=False)
    Month = Column(String(5), unique=False)
    UnemploymentRate = Column(Float, unique=False)


#   ESRI unemployment Data
class ESRI_Unemployment_Multiplier(Base):
    __tablename__ = "ESRI_Unemployment_Multiplier"

    Geo_ID = Column(String(10), unique=False, primary_key=True)
    Geo_Name = Column(String(50), unique=False)
    Geo_Type = Column(String(50), unique=False)
    UnemploymentRate_BLS = Column(Float, unique=False)
    UnemploymentRate_ESRI = Column(Float, unique=False)
    Unemployment_multiplier = Column(Float, unique=False)

class ZIP_Adjustment_Multiplier(Base):
    __tablename__ = "ZIP_Adjustment_Multiplier"

    ZIP = Column(String(5), unique=False, primary_key=True)
    MSAID = Column(String(5), unique=False)
    COUNTYID = Column(String(5), unique=False)
    STATEID = Column(String(2), unique=False)
    ZIP_PriceChange = Column(Float, unique=False)
    MSA_PriceChange = Column(Float, unique=False)
    COUNTY_PriceChange = Column(Float, unique=False)
    USA_PriceChange = Column(Float, unique=False)
    County_UnemploymentRate = Column(Float, unique=False)
    Msa_UnemploymentRate = Column(Float, unique=False)
    National_UnemploymentRate = Column(Float, unique=False)
    Metro_unemployment_multiplier = Column(Float, unique=False)
    State_Unemployment_multiplier = Column(Float, unique=False)



class MSA_HomeValue_Multiplier(Base):
    __tablename__ = "MSA_HomeValue_Multiplier"

    MSAID = Column(String(5), unique=False, primary_key=True)
    MSA_PriceChange = Column(Float, unique=False)

class County_HomeValue_Multiplier(Base):
    __tablename__ = "County_HomeValue_Multiplier"

    COUNTYID = Column(String(5), unique=False, primary_key=True)
    COUNTY_PriceChange = Column(Float, unique=False)


class Zillow_MSAID_Lookup(Base):
    __tablename__ = "Zillow_MSAID_Lookup"

    Zillow_Id = Column(String(10), unique=False, primary_key=True)
    Zillow_MSA_Name = Column(String(50), unique=False)
    Geo_ID = Column(String(10), unique=False)
    MSA_Name = Column(String(50), unique=False)


class Zipcode_to_County_MSA(Base):
    __tablename__ = "Zipcode_to_County_MSA"

    ZIP = Column(String(5), unique=False, primary_key=True)
    COUNTYID = Column(String(5), unique=False)
    MSAID = Column(String(5), unique=False)



class InitiateDeclaratives():
    @staticmethod
    def create_tables(engine_string):
        engine = create_engine(engine_string)
        Base.metadata.create_all(engine)


