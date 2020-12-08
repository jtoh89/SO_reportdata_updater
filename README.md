# SO_reportdata_updater


libraries:

arcgis
openpyxl
geocoder
sqlalchemy
pymysql
xlrd


Update MSA Names from BLS:
1. Run get_msa_names.py


Update Geo Mappings
1. Run import_esri_msa_to_county.py
2. Run import_zip_mappings.py


Unemployment Update Directions:
1. Edit current_month, US_UNEMPLOYMENT in BLS_scraper.py
2. Run BLS_scraper.py
3. ESRI_Msa_Unemployment_Adjustment.py
4. Run ESRI_County_Unemployment_Adjustment.py

ZIP Update Directions:
1. Update Zillow files if needed
2. Run zillow_homevalue_parser.py
3. Run run_zipcode_adjuster.py