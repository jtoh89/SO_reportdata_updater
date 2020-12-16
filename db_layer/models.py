from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Table, MetaData, create_engine, Column, Integer, String, Float, BigInteger, Date

Base = declarative_base()


class BLS_Unemployment(Base):
    __tablename__ = "BLS_Unemployment"

    Geo_ID = Column(String(10), unique=False, primary_key=True)
    Geo_Name = Column(String(150), unique=False)
    Geo_Type = Column(String(50), unique=False)
    Year = Column(Integer, unique=False)
    Month = Column(String(5), unique=False)
    UnemploymentRate = Column(Float, unique=False)

class ESRI_Unemployment_Adjustments(Base):
    __tablename__ = "ESRI_Unemployment_Adjustments"

    Geo_ID = Column(String(10), unique=False, primary_key=True)
    Geo_Name = Column(String(150), unique=False)
    Geo_Type = Column(String(50), unique=False)
    UnemploymentRate_BLS = Column(Float, unique=False)
    UnemploymentRate_ESRI = Column(Float, unique=False)
    Unemployment_Adjustment = Column(Float, unique=False)

class ZIP_MacroData_Update(Base):
    __tablename__ = "ZIP_MacroData_Update"

    ZIP = Column(String(5), unique=False, primary_key=True)
    MSAID = Column(String(5), unique=False)
    COUNTYID = Column(String(5), unique=False)
    STATEID = Column(String(2), unique=False)
    ZIP_PriceChange = Column(Float, unique=False)
    COUNTY_PriceChange = Column(Float, unique=False)
    MSA_PriceChange = Column(Float, unique=False)
    USA_PriceChange = Column(Float, unique=False)
    COUNTY_UnemploymentRate = Column(Float, unique=False)
    MSA_UnemploymentRate = Column(Float, unique=False)
    USA_UnemploymentRate = Column(Float, unique=False)
    MSA_Unemployment_Adjustment = Column(Float, unique=False)
    STATE_Unemployment_Adjustment = Column(Float, unique=False)
    County_Unemployment_Adjustment = Column(Float, unique=False)

class HomeValue_PriceChange(Base):
    __tablename__ = "HomeValue_PriceChange"

    Geo_ID = Column(String(5), unique=False, primary_key=True)
    Geo_Type = Column(String(50), unique=False)
    PriceChange = Column(Float, unique=False)

class Zillow_MSAID_Lookup(Base):
    __tablename__ = "Zillow_MSAID_Lookup"

    Zillow_Id = Column(String(10), unique=False, primary_key=True)
    Zillow_MSA_Name = Column(String(150), unique=False)
    Geo_ID = Column(String(10), unique=False)
    MSA_Name = Column(String(150), unique=False)

class GeoMapping_Zipcode_to_CountyMSAState(Base):
    __tablename__ = "GeoMapping_Zipcode_to_CountyMSAState"

    ZIP = Column(String(5), unique=False, primary_key=True)
    COUNTYID = Column(String(5), unique=False)
    MSAID = Column(String(5), unique=False)
    STATEID = Column(String(2), unique=False)

class GeoMapping_MSA_to_CountyState(Base):
    __tablename__ = "GeoMapping_MSA_to_CountyState"

    ID = Column(String(10), unique=False, primary_key=True)
    MSAID = Column(String(5), unique=False)
    COUNTYID = Column(String(5), unique=False)
    STATEID = Column(String(2), unique=False)

#########################
###   Backup Tables
#########################

class BACKUP_BLS_Unemployment(Base):
    __tablename__ = "BACKUP_BLS_Unemployment"

    ID = Column(Integer, unique=False, primary_key=True, autoincrement=True)
    Geo_ID = Column(String(5), unique=False)
    Geo_Name = Column(String(150), unique=False)
    Geo_Type = Column(String(50), unique=False)
    Year = Column(Integer, unique=False)
    Month = Column(String(5), unique=False)
    UnemploymentRate = Column(Float, unique=False)
    Backup_Date = Column(Date, unique=False)

class BACKUP_ESRI_Unemployment_Adjustments(Base):
    __tablename__ = "BACKUP_ESRI_Unemployment_Adjustments"

    ID = Column(Integer, unique=False, primary_key=True, autoincrement=True)
    Geo_ID = Column(String(5), unique=False)
    Geo_Name = Column(String(150), unique=False)
    Geo_Type = Column(String(50), unique=False)
    UnemploymentRate_BLS = Column(Float, unique=False)
    UnemploymentRate_ESRI = Column(Float, unique=False)
    Unemployment_Adjustment = Column(Float, unique=False)
    Backup_Date = Column(Date, unique=False)


class BACKUP_ZIP_MacroData_Update(Base):
    __tablename__ = "BACKUP_ZIP_MacroData_Update"

    ID = Column(Integer, unique=False, primary_key=True, autoincrement=True)
    ZIP = Column(String(5), unique=False)
    MSAID = Column(String(5), unique=False)
    COUNTYID = Column(String(5), unique=False)
    STATEID = Column(String(2), unique=False)
    ZIP_PriceChange = Column(Float, unique=False)
    COUNTY_PriceChange = Column(Float, unique=False)
    MSA_PriceChange = Column(Float, unique=False)
    USA_PriceChange = Column(Float, unique=False)
    COUNTY_UnemploymentRate = Column(Float, unique=False)
    MSA_UnemploymentRate = Column(Float, unique=False)
    USA_UnemploymentRate = Column(Float, unique=False)
    MSA_Unemployment_Adjustment = Column(Float, unique=False)
    STATE_Unemployment_Adjustment = Column(Float, unique=False)
    County_Unemployment_Adjustment = Column(Float, unique=False)
    Backup_Date = Column(Date, unique=False)

class BACKUP_HomeValue_PriceChange(Base):
    __tablename__ = "BACKUP_HomeValue_PriceChange"

    ID = Column(Integer, unique=False, primary_key=True, autoincrement=True)
    Geo_ID = Column(String(5), unique=False)
    Geo_Type = Column(String(50), unique=False)
    PriceChange = Column(Float, unique=False)
    Backup_Date = Column(Date, unique=False)





class InitiateDeclaratives():
    @staticmethod
    def create_tables(engine_string):
        engine = create_engine(engine_string)
        Base.metadata.create_all(engine)


