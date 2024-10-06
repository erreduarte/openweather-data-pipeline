# openweather-data-pipeline
This project uses Apache Airflow to extract weather data from the OpenWeather API, process it with PySpark, and load it into Azure SQL Database. Intermediate data is stored in Azure Blob Storage. The pipeline runs hourly, automating the ETL process for multiple cities, and showcases a cloud-based data engineering solution.
