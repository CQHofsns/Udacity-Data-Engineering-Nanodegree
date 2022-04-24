import regex as re
import datetime as dt

from pyspark.sql.functions import udf, col, mean, lower, avg, round
from pyspark.sql.functions import year, month, dayofmonth
from pyspark.sql.types import IntegerType as Int, TimestampType

def immigration_validation(df):
    # Read valid port from I94_SAS_Labels_Descriptions
    get_valid_port= re.compile(r'\'(.*)\'.*\'(.*)\'')
    valid_port_dict= {'port_code': [], 'port_name': []}
    with open('i94validport.txt') as f:
        for line in f:
            match= get_valid_port.search(line)
            valid_port_dict['port_code'].append(match[1])
            valid_port_dict['port_name'].append(match[2].split(',')[0].lower())

    valid_port= list(valid_port_dict['port_code'])

    # Read valid CIT from I94_SAS_Labels_Descriptions
    get_valid_cit= re.compile(r'(.*).*\'(.*)\'')
    valid_cit_dict= {'origin_country_code': [], 'origin_country': []}
    with open('i94validcit.txt') as f:
        for line in f:
            match= get_valid_cit.search(line)
            valid_cit_dict['origin_country_code'].append(float(match[1][:3]))
            valid_cit_dict['origin_country'].append(match[2].lower())

    valid_cit= list(valid_cit_dict['origin_country_code'])  

    # Read valid Address from I94_SAS_Labels_Descriptions
    get_valid_addr= re.compile(r'\'(.*)\'.*\'(.*)\'')
    valid_addr_dict= {'state_code': [], 'state': []}
    with open('i94validaddr.txt') as f:
        for line in f:
            match= get_valid_addr.search(line)
            valid_addr_dict['state_code'].append(match[1][:4])
            valid_addr_dict['state'].append(match[2].lower())

    valid_addr= list(valid_addr_dict['state_code'])

    df_valid= df.filter(
        (col('i94port').isin(valid_port)) &
        (col('i94cit').isin(valid_cit)) & 
        (col('i94res').isin(valid_cit)) &
        (col('i94addr').isin(valid_addr))
    )
    
    return df_valid

def preprocess_immigration_dataset(imda_df):
    
    # Based on I94_SAS_Labels_Description.SAS file conduct data validation
    imda_df_valid= immigration_validation(imda_df)
    
    # Drop duplicated primary key value:
    imda_df_dropdup= imda_df_valid.drop_duplicates(['cicid'])
    
    # Drop most missing or null appear columns
    drop_columns= ['entdepu', 'occup', 'insnum']

    imda_df_dropcols= imda_df_dropdup.drop(*drop_columns)
    
    # Fillout gender and visapost columns with missing or null data with 'Not mentioned' value
    imda_df_fillna= imda_df_dropcols.na.fill('Not mentioned', subset= ['gender', 'visapost'])
    
    # Drop rows that all values is missing or null included
    imda_df_final= imda_df_fillna.dropna(how='all')
    
    # Convert arrdate column's value from SAS format into Georgian format
    sasdate_converter= udf(
        lambda x: dt.datetime(1960, 1, 1) + dt.timedelta(days= int(x)),
        TimestampType()
    )

    imda_df_final= (
        imda_df_final
        .withColumn('arrival_date', sasdate_converter('arrdate'))
        .withColumn('arrival_year', year(sasdate_converter('arrdate')))
        .withColumn('arrival_month', month(sasdate_converter('arrdate')))
        .withColumn('arrival_day', dayofmonth(sasdate_converter('arrdate')))
    )
    
    return imda_df_final

def preprocess_aircode_dataset(ac_df):
    
    # Drop all duplicated primary key value:
    ac_df_dropdup= ac_df.drop_duplicates(['ident'])
    
    # Spliting iso_region into country and region and keep region apart only
    split_region= udf(
        lambda x: x.split('-')[1]
    )
    
    ac_df_region= ac_df_dropdup.withColumn('iso_region', split_region('iso_region'))
    
    # Drop columns that contain many missing or null values:
    drop_columns= ['iata_code', 'local_code', 'gps_code']

    ac_df_dropcols= ac_df_region.drop(*drop_columns)

    # Replace all the missing or null values in the elevation_ft column with the mean value of available value
    elevation_mean_df= ac_df_dropcols.select(mean(col('elevation_ft')).alias('elevation_mean')).collect()

    elevation_mean= elevation_mean_df[0]['elevation_mean']
    
    ac_elevation_fillna= ac_df_dropcols.na.fill(elevation_mean, subset= ['elevation_ft'])
    
    ac_df_final= ac_elevation_fillna.dropna(how='all')
    
    return ac_df_final

def preprocess_us_cities_demographic_dataset(uscd_df):
    
    # Drop all rows in the subset that contain missing or null value
    uscd_df_final= uscd_df.dropna(
        subset=[
            'Average Household Size', 
            'Number of Veterans', 
            'Foreign-born', 
            'Male Population',
            'Female Population'
        ]
    )
    
    # Lowercase all the value in the City column in order to easy to match value with other dataset
    uscd_df_final= uscd_df_final.withColumn('City', lower(col('City')))
    
    return uscd_df_final

def preprocess_world_temperature_dataset(wtd_df):
    
    # Break the dt column into 3 different column Year, Month, and Day:
    wtd_df_ymd= (
        wtd_df
        .withColumn('record_year', year('dt'))
        .withColumn('record_month', month('dt'))
        .withColumn('record_day', dayofmonth('dt'))
    )
    
    # Filter data that only from 2011 and only recored in the US
    wtd_df_filter= wtd_df_ymd.filter((col('record_year') >= 2011) & (col('Country')== 'United States')).drop('Country')

    # Drop rows that cotain missing or null values in this subset:
    wtd_df_dropna= wtd_df_filter.dropna(
        subset=[
            'AverageTemperature', 
            'AverageTemperatureUncertainty'
        ]
    )
    
    # Lowercase all the value (city) that appear in the City column and calculate the average temperature with its uncertainty groupby every city of the US
    wtd_df_final= wtd_df_dropna.select([lower(col('City')).alias('us_city'), 'AverageTemperature', 'AverageTemperatureUncertainty'])
    wtd_df_final= wtd_df_final.groupBy('us_city').agg(round(avg('AverageTemperature'), 2).alias('avg_temperature'), round(avg('AverageTemperatureUncertainty'), 2).alias('avg_temperature_uncertainty'))

    return wtd_df_final


