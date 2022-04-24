from pyspark.sql.types import StructType as R, StructField as Fld, DoubleType as Dbl, StringType as Str, IntegerType as Int

def load_immigration_dataset(spark, imda_file_path):
    imda_df = spark.read.\
    format('com.github.saurfang.sas.spark').\
    option('header', True).\
    load(imda_file_path)
    
    return imda_df

def load_aircode_dataset(spark, ac_file_path):
    
    # Define schema
    ac_Schema= R([
        Fld('ident', Str()),
        Fld('type', Str()),
        Fld('name', Str()),
        Fld('elevation_ft', Dbl()),
        Fld('continent', Str()),
        Fld('iso_country', Str()),
        Fld('iso_region', Str()),
        Fld('municipality', Str()),
        Fld('gps_code', Str()),
        Fld('iata_code', Str()),
        Fld('local_code', Str()),
        Fld('coordinates', Str())
    ])
    
    # Load the dataset from CSV file
    ac_df= spark.read.\
    format('csv').\
    option('header', True).\
    schema(ac_Schema).\
    load(ac_file_path)

    return ac_df

def load_us_cities_demographic_dataset(spark, uscd_file_path):
    
    # Define Schema
    uscd_Schema= R([
        Fld('City', Str()),
        Fld('State', Str()),
        Fld('Median Age', Dbl()),
        Fld('Male Population', Int()),
        Fld('Female Population', Int()),
        Fld('Total Population', Int()),
        Fld('Number of Veterans', Int()),
        Fld('Foreign-born', Int()),
        Fld('Average Household Size', Dbl()),
        Fld('State Code', Str()),
        Fld('Race', Str()),
        Fld('Count', Int())
    ])
    
    uscd_df= spark.read.\
    format('csv').\
    option('header', True).\
    options(delimiter= ';').\
    schema(uscd_Schema).\
    load(uscd_file_path)
    
    return uscd_df


def load_world_temperature_dataset(spark, wtd_file_path):
    
    # Define Schema
    wtd_Schema= R([
        Fld('dt', Str()),
        Fld('AverageTemperature', Dbl()),
        Fld('AverageTemperatureUncertainty', Dbl()),
        Fld('City', Str()),
        Fld('Country', Str()),
        Fld('Latitude', Str()),
        Fld('Longitude', Str())
    ])
    
    wtd_df= spark.read.\
    format('csv').\
    option('header', True).\
    schema(wtd_Schema).\
    load(wtd_file_path)
    
    return wtd_df


def main_load_process(spark):
    imda_file_path= '../../data/18-83510-I94-Data-2016/i94_apr16_sub.sas7bdat'
    ac_file_path= 'airport-codes_csv.csv'
    uscd_file_path= 'us-cities-demographics.csv'
    wtd_file_path= '../../data2/GlobalLandTemperaturesByCity.csv'
    
    imda_df= load_immigration_dataset(spark, imda_file_path)
    ac_df= load_aircode_dataset(spark, ac_file_path)
    uscd_df= load_us_cities_demographic_dataset(spark, uscd_file_path)
    wtd_df= load_world_temperature_dataset(spark, wtd_file_path)
    
    return imda_df, ac_df, uscd_df, wtd_df
