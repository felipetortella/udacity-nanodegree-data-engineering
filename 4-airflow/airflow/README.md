# Airflow Data Pipeline - Sparkify

## 1. Project Description
A music streaming company, Sparkify, has decided that it is time to introduce more automation and monitoring to their data warehouse ETL pipelines and come to the conclusion that the best tool to achieve this is Apache Airflow.

They have decided to bring you into the project and expect you to create high grade data pipelines that are dynamic and built from reusable tasks, can be monitored, and allow easy backfills. They have also noted that the data quality plays a big part when analyses are executed on top the data warehouse and want to run tests against their datasets after the ETL steps have been executed to catch any discrepancies in the datasets.

The source data resides in S3 and needs to be processed in Sparkify's data warehouse in Amazon Redshift. The source datasets consist of JSON logs that tell about user activity in the application and JSON metadata about the songs the users listen to.

## 2. DAG Configuration
The image below is the configuration in airflow of this DAG pipeline.
![Airflow DAG Pipeline](../images/dag.png)

### 2.1 Stage events 
This step is responsible to extract the data from S3 and load it into stage tables in Redshift, making it easier to work with them to load the fact/dimensions tables.

### 2.2 Fact/Dimensions tables
This step is responsible to load the data into fact/dimensions tables. This step also make some transformations in data to load them.
This new struct is based on star schema as we can see in the image below.
![Stage tables and Star schema](../images/database.png)

### 2.3 Fact/Dimensions tables
This step is responsible to make data quality checks. For this step we are checking if there is data on fact/dimensions tables.
We make it simple, but this step could also make a lot of other checks that could guarantee a better pipeline.

## 3. Project organization
```
project
│
└───images  - images used on readme file
└───airflow - root project folder
└──────dags - DAGs folder
|         elt_dag.py - The project DAG
└──────plugins - Airflow custom plugins folder
└─────────helpers - Utils folder
│             sql_queries.py - ETL queries.
└─────────operators - Airflow custom operators folder
│             data_quality.py - Data quality operator.
│             load_dimension.py - Dimension tables load operator.
│             load_fact.py - Fact table load operator.
│             stage_redshift.py - Staging tables load operator.
|      create_tables.sql - SQL statements used to create the datawarehouse tables
│      README.md - This README file.
```

## 4. Run the project
1. The first step to run Airflow project is to install airflow locally. You will need to point the DAG folder of this project etl_dag.py or copy this file into DAG folder of airflow.
You will also need to do the same steps to plugins folder.
2. Configure Airflow connections. You will need to configure 2 connections:
     - redshift
     - aws_credentials
3. Create the tables into Redshit using create_tables.sql file. 
4. Connect to Airflow UI and run the DAG (etl_dag)
