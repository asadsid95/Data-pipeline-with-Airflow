# Project: Data Lake

## Background

Note: This project builds onto the client, Sparkify's evolving requirements from project 1, 2, 3 and 4.

In the first 2 projects, Sparkify needed Postgres/Cassandra database with ETL pipeline to process locally-hosted data, transform and load it into the databases. The data included user activity, and metadata of the songs in their app. The data was provided in JSON.

Then Sparkify's requirement (in Project 3) evolved to requiring staging tables which their Analytics team would use to find insights. This included data being located on Amazon S3 from which ETL pipeline would be directed to Amazon Redshift. Star schema was used for staging the tables, to enhance usability for the Analytics team. Consequently, Project 4 addressed their growing user and songs database by leveraging Data Lake.

**For this project**, the Data Engineering team is tasked with introducing automation, monitoring and quality assurance in Sparkify's existing data pipelines. 

Data Engineering team will be conducting 3 tasks entirely through Apache Airflow, using custom operators and DAG: 

1. Access Sparkify's public S3 bucket to retrieve user activity and songs data, and stage it in AWS Redshift (Redshift)
2. Using Redshift, data is extracted and transformed according to fact and dimension tables' attributes
3. Data quality checks are then performed 

## Data Pipelines's Design 

The pipeline (in other words, DAG) will consists of 4 custom operators:
- StageToRedshiftOperator
- LoadFactOperator
- LoadDimensionOperator
- DataQualityOperator

**Before describing the tables, the datsets for user activity and songs metadata are described:**

### Staging table's attributes/columns, data types are as follows:

Name of dataset: log_data
Attribute/Data-Type/Constraints (if applicable):

- artist: string
- auth: string
- firstName: string
- gender: string
- itemInSession: long
- lastName: string
- length: double
- level: string
- location: string
- method: string
- page: string 
- registration: double 
- sessionId: long 
- song: string
- status: long 
- ts: long
- userAgent: string
- userId: string 

Name of table: song_data
Attribute/Data-Type/Constraints (if applicable):

- artist_id: string
- artist_latitude: double 
- artist_location: string 
- artist_longitude: double
- artist_name: string
- duration: double
- num_songs: long
- song_id: string
- title: string
- year: long

### Dimensional table attributes/columns, data types and constraints are as follows:

Name of table: songs
Attribute/Data-Type/Constraints (if applicable):
Partitioned by (\*): year & artist_id
Distinct attribute: song_id

- song_id: string
- title: string
- artist_id: string \*
- year: long \*
- duration: double 

Name of table: artists
Attribute/Data-Type/Constraints (if applicable):
Partitioned by: None
Distinct attribute: artist_id

- artist_id: string
- artist_name: string
- artist_location: string 
- artist_latitude: double 
- artist_longitude: double

Name of table: users
Attribute/Data-Type/Constraints (if applicable):
Partitioned by: None
Distinct attribute: user_id

- user_id string 
- first_name string 
- last_name string 
- gender string 
- level string 

Name of table: time
Attribute/Data-Type/Constraints (if applicable):
Partitioned by (\*): year & month
Distinct attribute: timestamp

- start_time: timestamp
- date: string
- hour: integer 
- day: integer 
- week: integer 
- month: integer \*
- year: integer \*
- day_of_week: integer 

Name of table: songplays
Attribute/Data-Type/Constraints (if applicable):
Partitioned by (\*): year & month

- user_id: string
- level: string
- song_id: string 
- artist_id: string 
- session_id: long 
- location: string
- user_agent: string
- year: integer \*
- month: integer \*

## How to run the scripts

To run this project:

*The are the following assumptions:*
- AWS IAM role have been created
- Connection for AWS credentials is created (either using Airflow's CLI or UI); Conn Id, Conn Type (Amazon Web Services), Login (AWS Access key), Password (AWS Secret key) 
- Connection for Redshift credentials is created (either using Airflow's CLI or UI); Conn Id, Conn Type (Postgres), Host (Data Warehouse's Endpoint URL), Schema, Login, Password
- Redshift cluster is launched and available
- Table schemas are created in Data Warehouses for staging tables, and fact and dimension tables. These can be found in /home/workspace/airflow/create_tables.sql

1. Run the Airflow webserve using '/opt/airflow/start.sh'
1. Using the toggle button, turn 'on' DAG.

## Curiosity question
 *Purpose of this section is to list questions that occurred to me while doing this project but I was unable to find answers for them
 
1. What are common data quality checks used when production pipelines?

2. How can pipeline's owner be notified via email upon pipeline failure?
- I wasn't able to understand how to set up credentials for my email, in order to accomplish this

3. How would pipelines be created if data resides on a local or not-always available host (unlike S3)?
