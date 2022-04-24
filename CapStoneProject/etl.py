import os
import load_dataset, preprocess

from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import StringType as Str, IntegerType as Int

def create_fact_immigration_table(imda_df_final, output_path):
    
    # Rename and re-define columns in the dataset
    imda_df_final= imda_df_final.select(
        col('cicid').cast(Int()).alias('cic_id'),
        col('i94yr').cast(Int()).alias('entry_year'),
        col('i94mon').cast(Int()).alias('entry_month'),
        col('i94cit').cast(Int()).alias('origin_country_code'),
        col('i94res').cast(Int()).alias('origin_resident_country_code'),
        col('i94port').alias('port_code'),
        col('arrdate').cast(Int()).alias('arrival_sas_date'),
        col('i94mode').cast(Int()).alias('entry_mode'),
        col('i94addr').alias('address_state_code'),
        col('i94visa').cast(Int()).alias('visa_mode'),
        col('biryear').cast(Int()).alias('birth_year'),
        col('admnum').cast(Int()).alias('addmission_number'),
        col('fltno').cast(Int()).alias('flight_number'),
        col('count').cast(Int()).alias('count'),
        'arrival_date',
        'arrival_year',
        'arrival_month',
        'arrival_day',
        'gender',
        'airline',
        'visatype'
    )

    # Create fact_immigration_table
    fact_immigration_table= imda_df_final.select(
        'cic_id',
        'entry_year',
        'entry_month',
        'port_code',
        'arrival_sas_date',
        'entry_mode',
        'address_state_code',
        'origin_country_code',
        'origin_resident_country_code',
        'visa_mode',
        'count',
        'birth_year',
        'gender',
        'airline',
        'addmission_number',
        'flight_number',
        'visatype'
    )

    # Create temporatory fact_immigration table for later quality check
    fact_immigration_table.createOrReplaceTempView('fact_immigration_temp')

    # Write the fact table
    fact_immigration_table.\
    write.\
    mode('overwrite').\
    partitionBy('entry_year', 'entry_month').\
    parquet(path= os.path.join(output_path, 'fact_immigration.parquet'))
    
    return fact_immigration_table, imda_df_final


def create_dim_aircode_table(ac_df_final, output_path):
    dim_aircode_table= ac_df_final.filter(col('iso_country') == 'US')
    dim_aircode_table.createOrReplaceTempView('dim_aircode_temp')
    dim_aircode_table.write.mode('overwrite').parquet(path= os.path.join(output_path, 'dim_aircode.parquet'))

    return dim_aircode_table

def create_dim_uscd_table(uscd_df_final, wtd_df_final, output_path):
    
    # Rename columns in the dataset
    uscd_df_final= uscd_df_final.select(
        col('City').alias('city'),
        col('State').alias('state'),
        col('Race').alias('race'),
        col('State Code').alias('state_code'),
        col('Median Age').alias('median_age'),
        col('Male Population').alias('male_population'),
        col('Female Population').alias('female_population'),
        col('Total Population').alias('total_population'),
        col('Number of Veterans').alias('number_veterans'),
        col('Foreign-born').alias('foreign_born'),
        col('Average Household Size').alias('avg_household_size'),
        col('Count').alias('count')
    )

    dim_uscd_table= uscd_df_final.join(wtd_df_final, uscd_df_final.city == wtd_df_final.us_city, 'left')

    dim_uscd_table.createOrReplaceTempView('dim_uscd_temp')

    dim_uscd_table.write.\
    mode('overwrite').\
    partitionBy('state', 'city', 'race').\
    parquet(path= os.path.join(output_path, 'dim_uscd.parquet'))
    
    return dim_uscd_table


def create_dim_arrivaldate_table(imda_df_final, output_path):
    
    dim_arrivaldate_table= imda_df_final.select(
        'arrival_sas_date',
        'arrival_date',
        'arrival_year',
        'arrival_month',
        'arrival_day'
    ).distinct()

    dim_arrivaldate_table.createOrReplaceTempView('dim_arrivaldate_temp')

    dim_arrivaldate_table.write.\
    mode('overwrite').\
    partitionBy('arrival_year', 'arrival_month').\
    parquet(path= os.path.join(output_path, 'dim_arrivaldate_table.parquet'))
    
    return dim_arrivaldate_table
    
    
def ETL_Pipeline():
    
    # ETL Process Step 0: Create Spark Session
    print(' --> [INFO] STEP 0: Creating Spark Session...')
    try:
        spark = SparkSession.builder.\
        config("spark.jars.repositories", "https://repos.spark-packages.org/").\
        config("spark.jars.packages", "saurfang:spark-sas7bdat:2.0.0-s_2.11").\
        enableHiveSupport().getOrCreate()
        
        print(' --> [SUCCESS] STEP 0: Spark Session created successfully!')
    except Exception as e0:
        print(f' --> [ERROR] STEP 0: Spark Session created failed: {e0}')
    # ETL Process Step 1: Scope the Project and Gather Data
    print(' --> [INFO] STEP 1: Gathering data is in process...')
    try:
        imda_df, ac_df, uscd_df, wtd_df= load_dataset.main_load_process(spark)
        print(' --> [SUCCESS] STEP 1 has been processed successfully !')
    except Exception as e1:
        print(f' --> [ERROR] STEP 1 has encountered an error: {e1}')
    
    # ETL Process Step 2: Preprocess each dataset:
    print(' --> [INFO] STEP 2: Data Preprocessing is in process...')
    try:
        imda_df_final= preprocess.preprocess_immigration_dataset(imda_df)
        ac_df_final= preprocess.preprocess_aircode_dataset(ac_df)
        uscd_df_final= preprocess.preprocess_us_cities_demographic_dataset(uscd_df)
        wtd_df_final= preprocess.preprocess_world_temperature_dataset(wtd_df)
        
        print(' --> [SUCCESS] STEP 2 has been processed successfully !')
    except Exception as e2:
        print(f' --> [ERROR] STEP 2 has encountered an error: {e2}')
        
    
    # ETL Process Step 3: Create and Write fact/dimension tables
    print(' --> [INFO] STEP 3: Data Modeling is in process...')
    try:
        output_path= '/home/workspace/output_data/'
        
        fact_immigration_table, imda_df_final_provised= create_fact_immigration_table(imda_df_final, output_path)
        dim_aircode_table= create_dim_aircode_table(ac_df_final, output_path)
        dim_uscd_table= create_dim_uscd_table(uscd_df_final, wtd_df_final, output_path)
        dim_arrivaldate_table= create_dim_arrivaldate_table(imda_df_final_provised, output_path)
        
        print(' --> [SUCCESS] STEP 3 has been processed successfully !')
    except Exception as e3:
        print(f' --> [ERROR] STEP 3 has encountered an error: {e3}')        
    
    return fact_immigration_table, dim_aircode_table, dim_uscd_table, dim_arrivaldate_table