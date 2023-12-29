# Importing the Python Libraries and Modules e.g. Pandas, PySpark etc.

import pandas as pd
import os
import pyspark.sql.functions as F
from pyspark.sql.functions import udf, col, to_timestamp
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear,dayofweek
from pyspark.sql.functions import col, lit
from pyspark.sql.types import *

from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.window import Window




def create_spark_session():
     
    """

    Main Module Name - etl.py

    Sub Module Name - create_spark_session

    Sub Module Name Description - This python module creates the Hadoop Spark Session for AWS:2.7.0 version.

    Input Parameters Details - N/A

    Output Parameters Details - 
    
    1.spark - This returns the spark session created for the "SparkSession" instance.
    
    """
    
    print("Starting the Spark Session")
    
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def extract_load_transform():
    
    """

    Main Module Name - etl.py

    Sub Module Name - transform

    Sub Module Name Description - This python module process the ETL pipeline for US Immigration data and stores them in a parquet file format.

    Input Parameters Details - N/A   


    Output Parameters Details - N/A
    
    """
    cwd = os.listdir()
    print("Current working directory:", cwd)
    
    print("Reading the Immigration Data in the Dataframe")
    
    df_immigration_data = pd.read_csv("/workspace/home/immigration_data_sample.csv")    
    
    # Reading the Global Temparature by City Data in the Dataframe
    df_temperature_data = pd.read_csv("../../data2/GlobalLandTemperaturesByCity.csv")    

    # Reading the US City Demographic Data in the Dataframe
    df_demographics_data = pd.read_csv("us-cities-demographics.csv",delimiter=';')    

    # Reading the Airports Code Data in the Dataframe
    df_airports_code_data = pd.read_csv("airport-codes_csv.csv")

    # Opening the SAS Label Descriptions File 
    with open("I94_SAS_Labels_Descriptions.SAS") as sas_file:
        file_contents = sas_file.readlines()

    # Splitting country code

    country_code = {}

    # For loop for in input country variable in the file contents of the SAS file
    for input_countries in file_contents[10:298]:
        # Splitting the contents
        pair = input_countries.split('=')
        # Code, country pair values
        code, country = pair[0].strip(), pair[1].strip().strip("'")
        # storing the final results
        country_code[code] = country

    # Converting the country code and country name from the list into the dataframe
    df_country_code = pd.DataFrame(list(country_code.items()), columns=['country_code', 'country_name'])        

    # Splitting the city code
    city_code = {}

    # For loop for in input city variable in the file contents of the SAS file
    for input_cities in file_contents[303:962]:
        # Splitting the contents
        pair = input_cities.split('=')
        # Code, City pair values
        code, city = pair[0].strip("\t").strip().strip("'"), pair[1].strip('\t').strip().strip("''")
        # storing the final results
        city_code[code] = city

    #  Getting the city code into the dataframe
    df_city_code = pd.DataFrame(list(city_code.items()), columns=['city_airport_code', 'city_name'])

    # Splitting the state code
    state_code = {}

    # For loop for in input states variable in the file contents of the SAS file
    for input_states in file_contents[982:1036]:
        # Splitting the contents
        pair = input_states.split('=')
        # Code, state pair values
        code, state = pair[0].strip('\t').strip("'"), pair[1].strip().strip("'")
        # storing the final results
        state_code[code] = state

    # Getting the state code and state name into the dataframe.
    df_state_code = pd.DataFrame(list(state_code.items()), columns=['state_code', 'state_name'])

    # Converting the Arrival Date and Departure Date into the required YYYY-MM-DD date format
    df_immigration_data['arrdate'] = pd.to_timedelta(df_immigration_data['arrdate'], unit='D') + pd.Timestamp('1960-1-1')
    df_immigration_data['depdate'] = pd.to_timedelta(df_immigration_data['depdate'], unit='D') + pd.Timestamp('1960-1-1')

    # Extracting the data columns for the FACT Immigration table 
    fact_immigration_table = df_immigration_data[['cicid', 'arrdate', 'i94res', 'i94port', 'i94addr', 'depdate', 'i94mode', 'i94visa','visatype','occup','gender','airline','fltno','biryear','admnum']]
    fact_immigration_table.columns = ['cic_id', 'arrive_date', 'origin_country_code', 'airport_code','state_code', 'departure_date', 'mode', 'visa','visa_type','occupation','gender','airline','airline_flight_number','date_of_birth_year','admission_id_number']

    # Extracting the data columns for the DIMENSION Demographics Table
    dimension_demographics_table = df_demographics_data[['State Code','City','Male Population','Female Population','Total Population','Median Age','Number of Veterans','Average Household Size','Count']]
    dimension_demographics_table.columns = ['state_code','city','male_population','female_population','total_population','median_age','number_of_veterans','average_household_size','total_count']

    # Extracting the data columns for the DIMENSION Country Temperature Table
    dimension_temp_countries_temperature_table = df_temperature_data[['dt','City','Country','AverageTemperature','Latitude','Longitude']]
    dimension_temp_countries_temperature_table.columns = ['temperature_date','city','country','average_temperature','latitude','longitude']
    dimension_temp_countries_temperature_table['country'] = dimension_temp_countries_temperature_table['country'].str.lower()
    df_country_code['country_name'] = df_country_code['country_name'].str.lower()

    # Extracting the data columns for the DIMENSION Airports Table
    dimension_airports_table = df_airports_code_data [['iata_code','name','type','local_code','coordinates']]
    dimension_airports_table.columns = ['airport_code','airport_name','airport_type','local_code','coordinates']

    # Extracting the data columns for the DIMENSION Arrival Date Table
    df_immigration_data['arrdate'] = pd.to_datetime(df_immigration_data['arrdate'])

    # Create a copy of the DataFrame to avoid SettingWithCopyWarning
    dimension_arrival_date_table = df_immigration_data[['arrdate']].copy()
    dimension_arrival_date_table.columns = ['arrival_date']

    # Convert 'arrival_date' to datetime format
    dimension_arrival_date_table['arrival_date'] = pd.to_datetime(dimension_arrival_date_table['arrival_date'])

    # Create new columns
    dimension_arrival_date_table['arrival_iso_date'] = dimension_arrival_date_table['arrival_date'].dt.strftime('%G-%V-%u').astype(str)
    dimension_arrival_date_table['arrival_season'] = dimension_arrival_date_table['arrival_date'].dt.quarter
    dimension_arrival_date_table['arrival_sas_date'] = dimension_arrival_date_table['arrival_date'].dt.strftime('%Y%m%d')
    dimension_arrival_date_table['arrival_day'] = dimension_arrival_date_table['arrival_date'].dt.day
    dimension_arrival_date_table['arrival_month'] = dimension_arrival_date_table['arrival_date'].dt.month
    dimension_arrival_date_table['arrival_year'] = dimension_arrival_date_table['arrival_date'].dt.year
                                                                                                   
    # Creating Spark Session "Udacity_Capstone_US_Immigration_Project" 
    spark = SparkSession.builder.appName("Udacity_Capstone_US_Immigration_Project").getOrCreate()

    # Define the schema for the Spark DataFrame based on Pandas DataFrame
    schema = StructType([
        StructField("cic_id", DoubleType(), True),
        StructField("arrive_date", DateType(), True),
        StructField("origin_country_code", DoubleType(), True),
        StructField("airport_code", StringType(), True),
        StructField("state_code", StringType(), True),
        StructField("departure_date", DateType(), True),
        StructField("mode", DoubleType(), True),
        StructField("visa", DoubleType(), True),
        StructField("visa_type", StringType(), True),
        StructField("occupation", StringType(), True),
        StructField("gender", StringType(), True),
        StructField("airline", StringType(), True),
        StructField("airline_flight_number", StringType(), True),
        StructField("date_of_birth_year", DoubleType(), True),
        StructField("admission_id_number", DoubleType(), True),
    ])

    # Convert Pandas DataFrame to Spark DataFrame
    spark_fact_immigration_table = spark.createDataFrame(fact_immigration_table, schema=schema)

    # Create or replace a temporary view
    spark_fact_immigration_table.createOrReplaceTempView("fact_immigration_table")

    # Now we can use Spark SQL queries on the created view
    result_fact_immigration = spark.sql("SELECT * FROM fact_immigration_table")

    # Define the schema for the Spark DataFrame for DIMENSION Arrival Date Table
    schema = StructType([
    StructField("arrival_date", DateType(), True),
    StructField("arrival_iso_date", StringType(), True),
    StructField("arrival_season", IntegerType(), True),
    StructField("arrival_sas_date", StringType(), True),
    StructField("arrival_day", IntegerType(), True),
    StructField("arrival_month", IntegerType(), True),
    StructField("arrival_year", IntegerType(), True)
    ])

    # Convert Pandas DataFrame to Spark DataFrame
    spark_dimension_arrival_date_table = spark.createDataFrame(dimension_arrival_date_table, schema=schema)

    # Create or replace a temporary view for Spark SQL queries
    spark_dimension_arrival_date_table.createOrReplaceTempView("dimension_arrival_date_table")

    # Now we can use Spark SQL queries on the "dimension_arrival_date_table" view
    result_dimension_arrival_date = spark.sql("SELECT * FROM dimension_arrival_date_table")
    dimension_countries_temperature_table = pd.merge(dimension_temp_countries_temperature_table, df_country_code, left_on='country', right_on='country_name', how='inner')

    # Define the schema for the Spark DataFrame for DIMENSION Airports Table
    schema = StructType([
    StructField("airport_code", StringType(), True),
    StructField("airport_name", StringType(), True),
    StructField("airport_type", StringType(), True),
    StructField("local_code", StringType(), True),
    StructField("coordinates", StringType(), True)
    ])

    # Convert Pandas DataFrame to Spark DataFrame
    spark_dimension_airports_table = spark.createDataFrame(dimension_airports_table, schema=schema)

    # Create or replace a temporary view for Spark SQL queries
    spark_dimension_airports_table.createOrReplaceTempView("dimension_airports_table")

    # Now we can use Spark SQL queries on the "dimension_airports_table" view
    result_dimension_airports = spark.sql("SELECT * FROM dimension_airports_table")

    # Convert columns with decimal values to float before using the float columns
    decimal_columns = ['male_population', 'female_population', 'number_of_veterans']
    dimension_demographics_table[decimal_columns] = dimension_demographics_table[decimal_columns].astype(float)

    # Define the schema for the Spark DataFrame for Dimension Demographics Table
    schema = StructType([
    StructField("state_code", StringType(), True),
    StructField("city", StringType(), True),
    StructField("male_population", FloatType(), True),
    StructField("female_population", FloatType(), True),
    StructField("total_population", IntegerType(), True),
    StructField("median_age", FloatType(), True),
    StructField("number_of_veterans", FloatType(), True),
    StructField("average_household_size", FloatType(), True),
    StructField("total_count", IntegerType(), True)
        ])

    # Convert Pandas DataFrame to Spark DataFrame
    spark_dimension_demographics_table = spark.createDataFrame(dimension_demographics_table, schema=schema)

    # Create or replace a temporary view for Spark SQL queries
    spark_dimension_demographics_table.createOrReplaceTempView("dimension_demographics_table")

    # Now we can use Spark SQL queries on the "dimension_demographics_table" view
    result_dimension_demographics = spark.sql("SELECT * FROM dimension_demographics_table")

    # Converting the temperature_date column to datetime
    dimension_countries_temperature_table['temperature_date'] = pd.to_datetime(dimension_countries_temperature_table['temperature_date'])

    # Define the schema for the Spark DataFrame
    schema = StructType([
    StructField("temperature_date", StringType(), True),
    StructField("city", StringType(), True),
    StructField("country", StringType(), True),
    StructField("average_temperature", FloatType(), True),
    StructField("latitude", StringType(), True),
    StructField("longitude", StringType(), True),
    StructField("country_code", StringType(), True),
    StructField("country_name", StringType(), True)
    ])

    # Convert Pandas DataFrame to Spark DataFrame
    spark_dimension_countries_temperature_table = spark.createDataFrame(dimension_countries_temperature_table, schema=schema)

    # Create or replace a temporary view for Spark SQL queries
    spark_dimension_countries_temperature_table.createOrReplaceTempView("dimension_countries_temperature_table")

    # Now we can use Spark SQL queries on the "dimension_countries_temperature_table" view
    result_dimension_countries_temperature = spark.sql("SELECT country_code ,country_name, average_temperature,latitude, longitude FROM dimension_countries_temperature_table limit 5")
    result_dimension_countries_temperature.show(2)

        # Output path for Parquet files
    output_path = "/workspace/home/output_folder"

    # Write Spark DataFrames to Parquet files
    spark_fact_immigration_table.write.mode("overwrite").parquet(f"{output_path}/fact_immigration_table")
    spark_dimension_arrival_date_table.write.mode("overwrite").parquet(f"{output_path}/dimension_arrival_date_table")
    spark_dimension_airports_table.write.mode("overwrite").parquet(f"{output_path}/dimension_airports_table")
    spark_dimension_demographics_table.write.mode("overwrite").parquet(f"{output_path}/dimension_demographics_table")
    spark_dimension_countries_temperature_table.write.mode("overwrite").parquet(f"{output_path}/dimension_countries_temperature_table")  
    
    print("ETL process completed")

def data_quality_checks():

    from pyspark.sql import SparkSession
    from pyspark.sql.functions import col, lit
    # Initialize Spark session with increased memory settings
    spark = SparkSession.builder \
        .appName("DataQualityChecks") \
        .config("spark.executor.memory", "4g") \
        .config("spark.driver.memory", "4g") \
        .getOrCreate()

    # Load the fact and dimension tables
    dq_fact_immigration_table = spark.table("fact_immigration_table")
    dq_dimension_arrival_date_table = spark.table("dimension_arrival_date_table")
    dq_dimension_airports_table = spark.table("dimension_airports_table")
    dq_dimension_demographics_table = spark.table("dimension_demographics_table")
    #dq_dimension_countries_temperature_table = spark.table("dimension_countries_temperature_table")

    # Data Quality Checks

    # 1. Check for Missing Values
    missing_values_check = lambda df, table_name: df.select([col(c).isNull().alias(c) for c in df.columns]) \
        .withColumn('table', lit(table_name)).limit(100)  # Adjust the limit as needed

    # 2. Check Data Types
    data_types_check = lambda df, table_name: df.select([col(c).cast("string").alias(c) for c in df.columns]) \
        .withColumn('table', lit(table_name)).limit(100)  # Adjust the limit as needed

    # 3. Duplicate Records
    duplicate_records_check = lambda df, table_name: df.groupBy(df.columns).count().filter(col("count") > 1) \
        .withColumn('table', lit(table_name)).limit(100)  # Adjust the limit as needed

    # 4. Referential Integrity
    referential_integrity_check = dq_fact_immigration_table.join(dq_dimension_arrival_date_table,
                                                                dq_fact_immigration_table["arrive_date"] == dq_dimension_arrival_date_table["arrival_date"], "left") \
        .filter(dq_dimension_arrival_date_table["arrival_date"].isNull()) \
        .withColumn('table', lit("fact_immigration_table")).limit(100)  # Adjust the limit as needed

    # 5. Data Distribution
    data_distribution_check = lambda df, column: df.groupBy(column).count() \
        .withColumn('table', lit(df.columns[0])).limit(100)  # Adjust the limit as needed

    # Check distribution of ages in demographics table
    age_distribution_check = data_distribution_check(dq_dimension_demographics_table, "median_age")

    # Execute Data Quality Checks
    data_quality_checks_results = []

    # Perform checks on each table
    for table_df, table_name in [
        (dq_fact_immigration_table, "fact_immigration_table"),
        (dq_dimension_arrival_date_table, "dimension_arrival_date_table"),
        (dq_dimension_airports_table, "dimension_airports_table"),
        (dq_dimension_demographics_table, "dimension_demographics_table")
        #,(dq_dimension_countries_temperature_table, "dimension_countries_temperature_table")
    ]:
        # 1. Check for Missing Values
        missing_values_result = missing_values_check(table_df, table_name)
        data_quality_checks_results.append(missing_values_result)

        # 2. Check Data Types
        data_types_result = data_types_check(table_df, table_name)
        data_quality_checks_results.append(data_types_result)

        # 3. Duplicate Records
        duplicate_records_result = duplicate_records_check(table_df, table_name)
        data_quality_checks_results.append(duplicate_records_result)

    # Display Results
    for result_df in data_quality_checks_results:
        # Check if there are any issues before displaying
        if result_df.count() > 0:
            table_name = result_df.select('table').distinct().collect()[0]['table']
            print(f"Data Quality Checks passed for {table_name}:")
            result_df.show(5)

    # Stop Spark session
    spark.stop()


def main():
    spark = create_spark_session()    
    extract_load_transform()    
    #data_quality_checks()


if __name__ == "__main__":
    main()
