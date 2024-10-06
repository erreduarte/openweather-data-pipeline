# OpenWeather Data Pipeline Using Apache Airflow, PySpark, Azure SQL, and Azure Blob Storage

This project demonstrates an end-to-end pipeline for extracting weather data from the OpenWeather API, transforming it using PySpark, and loading the results into Azure SQL Database. The project is orchestrated with Apache Airflow and makes use of Azure Blob Storage for intermediate file storage.

## Key Technologies
- **Apache Airflow**: Orchestrating the entire pipeline.
- **PySpark**: Processing and transforming the extracted data.
- **Azure Blob Storage**: Storing intermediate files.
- **Azure SQL Database**: Storing the final processed data.
- **OpenWeather API**: Source of weather data.

## Pipeline Overview

The pipeline follows these steps:
1. **Extract Data**: Fetches weather data for a list of cities from the OpenWeather API and loads it into a PySpark DataFrame.
2. **Process Data**: Normalizes the raw JSON data into a tabular format and saves the data in Azure Blob Storage.
3. **Transform Data**: Reads the processed data from Blob Storage, applies transformations, and prepares it for loading into Azure SQL Database.
4. **Create Staging Table**: A staging table is created in Azure SQL to temporarily hold the transformed data.
5. **Load Staging Table**: Bulk loads the transformed data from Blob Storage into the Azure SQL staging table.
6. **Load to DB**: Inserts new records into the main weather table, ensuring no duplicates.
7. **Clean-up**: Drops the staging table and deletes weather data older than 24 hours from the main table.

## Code Structure

### Main Dependencies:
- **Airflow Operators**: To define and orchestrate DAG tasks.
- **MsSqlHook**: To connect and interact with Azure SQL Database.
- **WasbHook**: For communication with Azure Blob Storage.
- **PySpark**: To handle data processing and transformations.

### Core DAG Tasks:
- `Extract_Data`: Extracts data from the OpenWeather API.
- `Process_Data`: Normalizes and stores the extracted data in Azure Blob Storage.
- `Transform_Data`: Transforms the data for analysis and prepares it for loading into Azure SQL.
- `Create_Staging_Table`: Creates a staging table in Azure SQL for temporary data storage.
- `Load_Staging_Table`: Bulk inserts data into the staging table from Azure Blob.
- `Load_to_DB`: Inserts unique records into the main weather data table.
- `Drop_Staging_Table`: Drops the staging table after data is loaded.
- `Drop_Old_Values`: Removes old records from the main table to keep only the latest 24 hours of data.

### Configuration:
The DAG uses a configuration file (`config.json`) containing sensitive information such as:
- API key for OpenWeather.
- Azure Blob Storage access.
- Azure SQL Database connection.

---

You can find the full code and detailed explanation of each component in the `open_weather_DAG.py` file in this repository.

## How to Run
1. Set up your environment with Airflow, PySpark, and the necessary cloud services (Azure Blob, Azure SQL).
2. Add your configuration in the `config.json` file (API keys, connection strings).
3. Trigger the DAG from the Airflow UI, and the pipeline will start extracting and processing data.

## Future Improvements
- Add monitoring and alerting for the DAG using Airflow sensors or external tools like Grafana.
- Scale up the pipeline for handling larger datasets or additional APIs.
