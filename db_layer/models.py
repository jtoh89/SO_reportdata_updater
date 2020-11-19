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
class BLS_Macrodata(Base):
    __tablename__ = "BLS_Macrodata"

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


