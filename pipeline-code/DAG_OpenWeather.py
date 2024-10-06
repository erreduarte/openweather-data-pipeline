import json
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
from airflow.operators.python import PythonOperator
from airflow.decorators import dag, task
from datetime import datetime, timedelta
import requests
import pandas as pd
from pyspark.sql import SparkSession
from airflow.providers.microsoft.azure.hooks.wasb import WasbHook
import io



#Dag Bag
@dag(
    schedule = '@hourly',
    start_date = datetime(2024, 10, 3),
    catchup= False,
    default_args={"owner": "owner_name",
                  },
    description="Extract data from OpenWeatherAPI, transform and load it to Azure SQL Database",
    tags = ["OpenWeatherAPI", "AzureSQLDatabase", "BlobStorage"]
        )



def open_weather_DAG(): 

    config_file = '/path/to/file/config.json'
    with open(config_file, 'r') as f:
            config=json.load(f)   
   
    
    db = config['azure_db']
    api = config['api_info']
    blob = config['blob_access']


         
    @task
    def Extract_Data():
         
        spark = SparkSession.builder\
                    .appName("Extract_OW_Data")\
                    .config("spark.sql.debug.maxToStringFields", 1000)\
                    .config("spark.jars.packages", "org.apache.hadoop:hadoop-azure:3.3.1")\
                    .config(f"fs.azure.sas.{blob['blob_container']}.{blob['blob_account']}.blob.core.windows.net", blob["SAS_key"]) \
                    .getOrCreate()
         
        try:
                #List comprehension to extract all values in a list            
                data_list = []
                for city in api['city_names']:
                    url = api['url_template'].format(city=city, API_KEY = api['API_key'])
                    response = requests.get(url)  

                    if response.status_code == 200:
                        data = response.json()
                        data_list.append(data)
                    else:
                        print(f"Error fetching data for {city}: {response.text}")

              
                return data_list

        except Exception as e:
             return f"An error occurred: {e}"

                

    @task    
    def Process_data(data_list):
                
            spark = SparkSession.builder\
                    .appName("Process_OW_Data")\
                    .config("spark.sql.debug.maxToStringFields", 1000)\
                    .config("spark.jars.packages", "org.apache.hadoop:hadoop-azure:3.3.1")\
                    .config(f"fs.azure.sas.{blob['blob_container']}.{blob['blob_account']}.blob.core.windows.net", blob["SAS_key"]) \
                    .getOrCreate()
                
            try:                
            
                    normalized_list = pd.json_normalize(data_list)
                    df = pd.DataFrame(normalized_list)
                    df_exploded = pd.json_normalize(df['weather'].explode()) #Explode JSON values from weather column
                    df_exploded = df_exploded.rename(columns={"id": "weather_id"}) #Rename 'id' column from exploded weather due to double names.
                    df_normalized = pd.concat([df.drop(columns=['weather']), df_exploded], axis=1) #Concat values
                    
                    print("JSON successfully normalized")

                    #Transform normalized data in a SparkDF to use better easier syntax when transforming the file (spark.sql)
                    df_final = spark.createDataFrame(df_normalized)

                    print("Spark Dataframe sucessfully created")          
        
                    #Save output to blob storage
                    output_path = f"wasbs://{blob['blob_container']}@{blob['blob_account']}.blob.core.windows.net/bronze_file"

                    df_final.coalesce(1).write.csv(output_path, mode="overwrite", header=True)      

            
            except Exception as e:
                return f"An error occured: {e}"
            else:
                spark.stop()
        
            finally:
                return f"Data sucessfully processed and saved in {blob['blob_container']}/bronze_file"
    
    @task
    def Transform_Data():

            spark = SparkSession.builder \
                        .appName("Transform_OW_Data")\
                        .config("spark.sql.debug.maxToStringFields", 1000) \
                        .config("spark.jars.packages", "org.apache.hadoop:hadoop-azure:3.3.1") \
                        .config(f"fs.azure.sas.{blob['blob_container']}.{blob['blob_account']}.blob.core.windows.net", blob["SAS_key"]) \
                        .config("fs.azure", "org.apache.hadoop.fs.azure.NativeAzureFileSystem") \
                        .getOrCreate()
        
            try:
        
                df_transform = spark.read\
                    .option("header", "true")\
                    .option("inferSchema", "true")\
                    .csv(f"wasbs://{blob['blob_container']}@{blob['blob_account']}.blob.core.windows.net/bronze_file/*.csv")
            
                df_transform.createOrReplaceTempView("weather_data")

                df_selected = spark.sql(""" 
                                                                
                            SELECT 
                                CAST(`dt`                       AS TIMESTAMP)              AS timestamp,
                                CAST(`id`                       AS INTEGER)                AS id,
                                `sys.country`                                              AS country, 
                                `name`                                                     AS city,
                                CAST(`main.temp`                AS FLOAT)                  AS main_temp,
                                CAST(`main.temp_min`            AS FLOAT)                  AS temp_min,
                                CAST(`main.temp_max`            AS FLOAT)                  AS temp_max,
                                CAST(`clouds.all`               AS INTEGER)                AS cloudiness,
                                CAST(`main.humidity`            AS FLOAT)                  AS humidity,
                                CAST(`wind.speed`               AS FLOAT)                  AS wind_speed,
                                cast(`rain.1h`                  AS FLOAT)                  AS rain,
                                `main`                                                     AS condition,
                                `description`                                              AS weather_description, 
                                CAST(`coord.lon`                AS FLOAT)                  AS longitude,
                                CAST(`coord.lat`                AS FLOAT)                  AS latitude               

                            FROM 
                                weather_data
                            WHERE
                                `dt` IS NOT NULL
                            ORDER BY 
                                `name` ASC
                                                                    
                            """
                            )
                
                df_final = df_selected.toPandas()
                        
                #Hold csv file in memory to load in Blob Storage
                csv_buffer = io.StringIO()
                df_final.to_csv(csv_buffer, index=False)
                print("File held in memory")

                #Load file to blob storage
                hook = WasbHook(wasb_conn_id = 'azure_blob_storage')

                hook.load_string(string_data = csv_buffer.getvalue(),
                container_name=blob['blob_container'],
                blob_name="Weather_Data__Silver.csv",
                overwrite = True)       

        
            except Exception as e:
                return f'An error occured: {e}'
            else:
                spark.stop()
            finally:
                return "Silver file saved in blob storage."

    
    #Retry set in order to wake up the database, because the first attempt will always fail.
    @task(retries = 5, retry_delay = timedelta(minutes=1))
    def Create_Staging_Table():
         
            hook = MsSqlHook(mssql_conn_id = db['conn_id'])

            create_staging_table = """
                    CREATE TABLE 
                        dbo.OpenWeatherData_staging(
                            timestamp smalldatetime,
                            id int,
                            country varchar(5),
                            city varchar(20),
                            main_temp float,
                            temp_min float,
                            temp_max float,
                            cloudiness int,
                            humidity float,
                            wind_speed float,
                            rain float,
                            main varchar(20),
                            weather_description varchar(35),
                            longitude float,
                            latitude float);                
                """
            
            try:
                hook.run(create_staging_table)
                return f"Staging table created successfully"

            except Exception as e:
                print(f"An error ocurred: {e}")
                raise
              
         

         
         
    @task
    def Load_Staging_Table(): #Populate staging table.
    
            hook = MsSqlHook(mssql_conn_id = db['conn_id']) 
         
            load_data = """
                BULK INSERT 
                    dbo.OpenWeatherData_staging
                FROM 
                    'Weather_Data__Silver.csv'
                WITH (
                    DATA_SOURCE     = 'OpenWeatherData',
                    ROWTERMINATOR   = '0x0a',
                    FIELDTERMINATOR = ',',
                    FIRSTROW        = 2,
                    CODEPAGE = '65001'
                        );
                        
                """
         
            try:
                hook.run(load_data)
                return "Table sucessfully populated"
          
            except Exception as e:
              return f"An error ocurred: {e}"

    
    
    
    @task
    def Load_to_DB(): #Load final table staging data, applying final transformations such as filtering out null values and avoiding double records.
      
        hook = MsSqlHook(mssql_conn_id = db['conn_id'])

        load_table = """
        INSERT INTO 
            dbo.OpenWeatherData 
                (timestamp, id, country, city, main_temp, temp_min, temp_max,cloudiness, humidity, 
                wind_speed, rain, main, weather_description, longitude, latitude)
        SELECT 
            s.timestamp, s.id, s.country, s.city, s.main_temp, s.temp_min, s.temp_max,s.cloudiness, s.humidity, 
            s.wind_speed, s.rain, s.main, s.weather_description, s.longitude, s.latitude
        FROM 
            dbo.OpenWeatherData_staging s
        WHERE NOT EXISTS(
            SELECT 1
            FROM 
                dbo.OpenWeatherData t
            WHERE 
                t.timestamp = s.timestamp
                AND t.id = s.id
                )
                AND s.timestamp IS NOT NULL;            
        
        """

        try:
            hook.run(load_table)
            return "Table sucessful populated"

        except Exception as e:
            return f"An error occurred: {e}"

    @task
    def Drop_Staging_Table(): #Right after the final table is populated, drop staging table
         hook = MsSqlHook(mssql_conn_id = db['conn_id'])

         drop_staging_table = "DROP TABLE OpenWeatherData_staging"

         try:
              hook.run(drop_staging_table)
              return "Staging table dropped"
         except Exception as e:
              return f"An error occurred {e}"
         
    @task
    def Drop_Old_Values(): #In order to save storage costs, drop values older than 24h.
        
        hook = MsSqlHook(mssql_conn_id = db['conn_id'])

        drop_values = """DELETE FROM 
                            dbo.OpenWeatherData
                          WHERE
                            timestamp < DATEADD(day, -1, GETDATE()); 
                            
                            """
        try:
            hook.run(drop_values)
            return "Values >24h dropped"
    
        except Exception as e:
              return f"An error occurred {e}"
            
         


    #Set dependencies
    Process_data(Extract_Data()) >> Transform_Data() >> Create_Staging_Table() >> Load_Staging_Table() >> Load_to_DB() >> [Drop_Staging_Table(), Drop_Old_Values()]


open_weather_DAG()





