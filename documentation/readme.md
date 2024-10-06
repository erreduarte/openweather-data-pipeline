## Pipeline Execution Overview

### Airflow DAG and Task Success
Below are screenshot details of the Airflow UI showing a successful DAG run for this pipeline:
<br>
<br>

##### -**DAG GENERAL VIEW:**
  
 1 -  General Details
 ![DAG Success](.screen-shots/dag_details.png) <br>
 
 2 - Graph Run VIew  
 ![DAG_Graph_Tree](.screen-shots/dag_run_success.png)

- **Tasks Logs**

Below are log details for each of the Airflow successful tasks run for this pipeline:

- TASK 1: Fetch Data 
![Fetch Data](.logs/1_extract_data_task.log) <br>

- TASK 2: Process Fetched Data
![Process Data](.logs/2_process_data.log) <br>

- TASK 3: Transform Data
![Transform_Data](.logs/3_transform_data.log) <br>

- TASK 4: Create Staging Table
![Create_Staging Table](.logs/4_create_staging_table.log) <br>

- TASK 5: Load Staging Table
![Load_Staging_Table](.logs/5_load_staging_table.log) <br>

- TASK 6: Populate Database
![Populate_Database](.logs/6_populate_db.log) <br>

- TASK 7: Drop values older than 24h
![Drop_values](.logs/7_drop_old_values.log) <br>

- TASK 8: Drop Staging Table
![Drop_Staging_Table](.logs/8_drop_staging_table.log) <br>


### Data Overview

This section provides an overview of the outcome data processed during the project. The folder contains three files that demonstrate the data at different stages:

1. ![WB_Data_Loans_Bronze](.data_view/Weather_Data__Bronze.csv): This file contains the raw data extracted from the OpenWather API, stored in blob files after processing fetched data.
   
2. ![WB_Data_Loans_Silver](.data_view/Weather_Data__Silver.csv): This file includes the transformed data.
   
3. A sample result of the final database query, illustrating how the transformed data was loaded into the database and can be queried.
   ![Database_View](.data_view/Azure_DB_Overview.png)

