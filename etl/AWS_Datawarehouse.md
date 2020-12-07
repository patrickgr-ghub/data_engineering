# Project: Data Warehouse on AWS
### Authored by: Patrick Groover
### Authored on: October 2020

#### Sources Include: Personal Project Work, Answers Provided within the "Mentor Help" Responses for this project within the Udacity Learning Portal, and Code Mentoring Sessions.

## Project Description
<p>This project simulates the Extraction, Transformation, and Loading (ETL) of "songs" and "user activity" on the new music streaming app for Sparkify. The analytics team would like to understand what songs users are currently listening to. The current data resides in a S3 bucket of user activity JSON logs and a directory of song metadata. The analytics team wants to move their processes and data onto the cloud to take advantage of the latest updates to database technologies and the ability to scale processes elasticly.</p>

<p>In order to optimize queries, the Data Engineer has created a database schema and ETL pipeline for loading information into data warehouses on AWS to be used within Redshift Analyses.</p>

### Visual Schema of the Project Tables:

<p>
    
![See Project Workspace File:'Schema_Data_Modeling_Postgres.png](http://na-sjdemo1.marketo.com/rs/786-GZR-035/images/Schema_Data_Modeling_Postgres.png "Visual of the Project Schema - Available in Project Workspace Files - 'Schema_Data_Modeling_Postgres.png' ")

</p>

<p>The Database Schema for the AWS Redshift ETL Pipeline reflects the final Fact and Dimension Tables that can be used to query information by the Analytics Team. It is important to note that staging tables were used to extract information from the "Songs" and "User Activity" Logs.</p>

### Staging Tables:
<p>"events_staging" Table
    <ul>
        <li>"artist", type: varchar</li>
        <li>"auth", type: varchar</li>
        <li>"firstName", type: varchar</li>
        <li>"gender", type: varchar</li>
        <li>"itemInSession", type: integer</li>
        <li>"lastName", type: varchar</li>
        <li>"length", type: float</li>
        <li>"level", type: varchar</li>
        <li>"location", type: varchar</li>
        <li>"method", type: varchar</li>
        <li>"page", type: varchar</li>
        <li>"registration", type: float</li>
        <li>"sessionId", type: integer</li>
        <li>"song", type: varchar</li>
        <li>"status", type: integer</li>
        <li>"ts", type: numeric</li>
        <li>"userAgent", type: varchar</li>
        <li>"userId", type: integer</li>
    </ul>
</p>

<p>"songs_staging" Table
    <ul>
        <li>"num_songs", type: integer</li>
        <li>"artist_id", type: varchar</li>
        <li>"artist_latitude", type: float</li>
        <li>"artist_longitude", type: float</li>
        <li>"artist_location", type: varchar</li>
        <li>"artist_name", type: varchar</li>
        <li>"song_id", type: varchar</li>
        <li>"title", type: varchar</li>
        <li>"duration", type: float</li>
        <li>"year", type: integer</li>
    </ul>
</p>
    
### Database Schema:
<p>"songplays" Fact Table
    <ul>
        <li>"sps_songplay_id", type: integer identity(1,1) NOT NULL</li>
        <li>"sps_ts", type: numeric NOT NULL</li> 
        <li>"sps_user_id", type: integer NOT NULL</li>
        <li>"sps_level", type: varchar</li> 
        <li>"sps_song_id", type: varchar distkey</li>
        <li>"sps_artist_id", type: varchar sortkey</li> 
        <li>"sps_session_id", type: integer</li> 
        <li>"sps_artist_location", type: varchar</li>
        <li>"sps_user_agent", type: varchar</li>
    </ul>
</p>

<p>"songs" Dimension Table
    <ul>
        <li>"song_id", type: varchar sortkey distkey</li>
        <li>"title", type: varchar</li>
        <li>"artist_id", type: varchar</li>
        <li>"year", type: integer</li>
        <li>"duration", type: numeric</li>
    </ul>
</p>

<p>"artists" Dimension Table
    <ul>
        <li>"artist_id", type: varchar NOT NULL sortkey</li>
        <li>"artist_name", type: varchar</li>
        <li>"artist_location", type: varchar</li>
        <li>"artist_latitude", type: numeric</li>
        <li>"artist_longitude", type: numeric</li>
    </ul>
</p>

<p>"users" Dimension Table
    <ul>
        <li>"user_id", type: integer sortkey</li>
        <li>"first_name", type: varchar</li>
        <li>"last_name", type: varchar</li>
        <li>"gender", type: varchar</li>
        <li>"level", type: varchar</li>
    </ul>
</p>
    
<p>"time" Dimension Table
    <ul>
        <li>"ts", type: timestamp NOT NULL sortkey</li>
        <li>"hour", type: integer</li>
        <li>"day", type: integer</li>
        <li>"week", type: integer</li>
        <li>"month", type: integer</li>
        <li>"year", type: integer</li>
        <li>"dayofweek", type: integer</li>
    </ul>
</p>

### Explanation of Sort & Dist Keys:
<p>To maintain data integrity and identify any inconsistencies that arise as table creation occurs, NOT NULL has been added to Primary Keys for each of the CREATE TABLE queries within "sql_queries.py". Distkeys and Sortkeys have been applied to fields and tables to optimize data processing times.</p>

> The "identity(1,1)" clause is use to create a unique identifier for each record within AWS Redshift.


### ETL Schema:

<p>To create the "songplays" Fact Table, the following ETL Schema was developed:</p>
    <ul>
        <li>File: "connection.py", Purpose: IaC framework to create the Redshift Cluster and Access Roles.</li>
        <li>File: "create_tables.py", Purpose: Drops & Creates Tables within the Redshift Database</li>
        <li>File: "sql_queries.py", Purpose: Queries to Drop, Create, and Insert Stagging & Songplays Data.</li>
        <li>File: "etl.py", Purpose: Executes the Finalized ETL Processes, Production Code.</li>
        <li>File: "ETL_Build.ipynb", Purpose: Jupyter Notebooks file used to build & validate Draft ETL Production Code.</li>
        <li>File: "delete_cluster.py", Purpose: Deletes Redshift Cluster & Roles.</li>
    </ul>


## Description of Data Handling & Functions

<p>Leverging Infrastructure as Code (IaC), the "connection.py" file will establish the AWS Role and Arn needed to make appropriate connections to Redshift.  The "crate_tables.py" file will Drop existing tables if needed, then setup the listed tables that include the staging tables, dimension tables, and final "songplays" Fact Table.  Finally, the "etl.py" file will be used to upload data from SONGS_DATA and LOG_DATA according the following extractions, transformations, and loads based on the process described below.</p>

<p>The "delete_cluster" table can be used at any time to eliminate the project cluster to restart, refine, and clean up the project.</p>

### 1. Establish the AWS IAM Role for Redshift Access & Create the Redshift Cluster

<p>The first step in delivering analytics capabilities through the Cloud will be to setup the Redshift Services via IaC using the "connection.py" file.</p>

<p>Begin by loading the "dwh.cfg" file and variables into local memory through the "configparser" library.</p>

> import configparser

> create local project variables

<p>
    <ul>
        <li>KEY = Secret Key for AWS Authentication</li>
        <li>SECRET = Secret Password for AWS Authentication</li>
        <li>DWH_CLUSTER_TYPE = Defines Cluster Type for IaC</li>
        <li>DWH_NUM_NODES = Defines Number of Nodes within Cluster Type for IaC</li>
        <li>DWH_NODE_TYPE = Defines Node Type for IaC</li>
        <li>DWH_IAM_ROLE_NAME = Sets the IAM Role Name for IaC</li>
        <li>DWH_CLUSTER_IDENTIFIER = Provides Cluster Identifier for IaC</li>
        <li>DWH_ROLE_ARN = Establishes roleArn for IaC and Redshift Connections</li>
        <li>SONG_DATA = Song Data S3 Bucket Path</li>
        <li>LOG_DATA = Log Data S3 Bucket Path</li>
        <li>LOG_JSONPATH = JSON Path to Interpret Log Data</li>
        <li>HOST = Path to Connect with Redshift Cluster</li>
        <li>DB_NAME = Cluster Name in Redshift</li>
        <li>DB_USER = User Name in Redshift</li>
        <li>DB_PASSWORD = Password to Connect with Redshift Cluster</li>
        <li>DB_PORT = Redshift Connection Port</li>
    </ul>
</p>

> create AWS Clients using boto3 Library

<p>
    <ul>
        <li>ec2 Client</li>
        <li>s3 Client</li>
        <li>iam Client</li>
        <li>redshift Client</li>
    </ul>
</p>

<p>Launch Functions to Create Role, Attach Policy, Get Role, and Create Cluster - Created the following functions in line with accessibility and scalability of IaC.</p>

> function "create_role" :: to create the IAM Role through IaC
> function "attach_policy" :: to attach IAM Role Policy
> function "get_role" :: to get & print the IAM Role
> function "create_cluster" :: to create the Redshift Cluster through IaC


### 2. Establish staging, dimension, and "songplays" tables using "create_tables.py"

<p>The first step in delivering the ETL schema is to CREATE the staging tables to import the SONG_DATA and LOG_DATA from the "udacity-dend" S3 bucket.</p>

<p>Load the appropriate python libraries</p>

> import psycopg2 :: postgreSQL Library
> from connection import * :: bring over local variables from "connection.py"
> from sql_queries import create_table_queries, drop_table_queries :: imports queries from "sql_queries.py"

<p>Drop Tables if they Exist:</p>

> def drop_tables(cur, conn):

<p>
    <ul>
        <li>Drops each table using the queries in `drop_table_queries` list.</li>
    </ul>
</p>

<p>Create staging, dimension, and "songplays" fact tables:</p>

> def create_tables(cur, conn):

<p>
    <ul>
        <li>Creates each table using the queries in `create_table_queries` list.</li>
    </ul>
</p>

<p>**It is important to note that the etl process is designed to load data into the "songs_staging" and "events_staging" tables in Redshift that will then be used to populate the dimension and fact tables.</p>

<p>Create Connection & Execute functions:</p>

> def main():

<p>
    <ul>
        <li>conn = creates connection</li>
        <li>cur = creates the cursor</li>
        <li>drop_tables(cur,conn) = execute drop_tables function</li>
        <li>create_tables(cur,conn) = execute create_tables function</li>
    </ul>
</p>

<p>Launch Connection & Functions</p>

> if__name__ == "__main__": main()


### 3. Access DROP, CREATE and INSERT queries within "sql_queries.py"

<p>The "create_tables.py" and "etl.py" files reference the "sql_queries.py" file through associated functions to deliver the supporting sql_queries that will DROP, CREATE and INSERT data into the appropriate tables, including the associated Fact and Dimension VALUES.</p>

<p>CREATE Queries Include:
    <ul>
        <li>songs_staging_table_create</li>
        <li>events_staging_table_create</li>
        <li>songplays_table_create</li>
        <li>songs_table_create</li>
        <li>artists_table_create</li>
        <li>users_table_create</li>
        <li>time_table_crate</li>
    </ul>
</p>

<p>The "artists" and "users" dimensions tables have the potential for duplicate records coming from the staging tables. To remove duplicates, "SELECT DISTINCT" has been used to de-duplicate the identity keys - resulting in unique records within each table.</p>

<p>COPY Queries Include:
    <ul>
        <li>events_staging_copy</li>
        <li>songs_staging_copy</li>
    </ul>
</p>

<p>The copy queries leverage the Local Variables of SONG_DATA, LOG_DATA, LOG_JSONPATH, and DWH_ROLE_ARN to connect to the appropriate "udacity-dend" S3 buckets and COPY the data into the staging tables.</p>

<p>INSERT Queries Include:
    <ul>
        <li>songplays_table_insert</li>
        <li>songs_table_insert</li>
        <li>artists_table_insert</li>
        <li>users_table_insert</li>
        <li>time_table_insert</li>
    </ul>
</p>

<p>The "songplays_table_insert statement uses INSERT INTO with JOINS on "artist_name", "title", and "duration" WHERE "page"="NextSong".</p>

#### Explanation of NOT NULL, sortkey and distkey by table:
<p>The Songs, Artists, Users, and Time Tables are large files and could take large amounts of time to process if ingested evenly across all Redshift Nodes. To optimize the ingestion of information to the dimension and "songplays" fact tables sortkeys and distkeys have been added to the following fields:
    <ul>
        <li>songs_table_create: sortkey and distkey for "sgs_song_id"</li>
        <li>artists_table_create: sortkey for "a_artist_id"</li>
        <li>users_table_create: sortkey for "u_user_id"</li>
        <li>time_table_create: sortkey for "t_ts"</li>
        <li>songsplays_table_create: sortkey for "sps_artist_id" and distkey for "sps_song_id"</li>
    </ul>
</p>

### 4. Load data to dimension tables & "songplays" fact table through "etl.py"

<p>The final step in delivering the ETL schema is to setup the dimension and fact tables will be to INSERT data from the staging tables into "songs", "artists", "users", "time", and "songplays".</p>


<p>Load the appropriate python libraries</p>

> import psycopg2 :: postgreSQL Library
> from connection import * :: bring over local variables from "connection.py"
> from sql_queries import copy_table_queries, insert_table_queries :: imports queries from "sql_queries.py"

<p>Load Staging Tables</p>

> def load_staging_tables(cur, conn):

<p>
    <ul>
        <li>Uses the COPY function to bring over data from "udacity-dend" S3 buckets.</li>
    </ul>
</p>

<p>INSERT data into dimension tables and "songplays" fact table:</p>

> def insert_tables(cur, conn):

<p>
    <ul>
        <li>Inserts data from staging tables into dimension tables & "songplays" fact table.</li>
    </ul>
</p>

<p>**It is important to note that the etl process is designed to load data into the "songs_staging" and "events_staging" tables in Redshift - these tables are then be used to populate the dimension and fact tables.</p>

<p>Create Connection & Execute functions:</p>

> def main():

<p>
    <ul>
        <li>conn = creates connection</li>
        <li>cur = creates the cursor</li>
        <li>drop_tables(cur,conn) = execute drop_tables function</li>
        <li>create_tables(cur,conn) = execute create_tables function</li>
    </ul>
</p>

<p>Launch Connection & Functions</p>

> if__name__ == "__main__": main()


### 5. Test the "songplays" ETL Schema

<p>To test the success of the ETL Schema, log into Redshift and attempt the following queries through the Redshift Query Editor:

<ul>
    <li>SELECT * FROM users LIMIT 10</li>
    <li>SELECT * FROM artists LIMIT 10</li>
    <li>SELECT * FROM songs LIMIT 10</li>
    <li>SELECT * FROM time LIMIT 10</li>
    <li>SELECT * FROM songplays LIMIT 10</li>
</ul>

</p>


### DATABASE CLEANUP file "delete_cluster.py"

<p>To reduce the cost of running servers and systems through AWS, data engineers can access the "delete_cluster.py" file. This file has the following design and functions:</p>

<p>Load the appropriate python libraries</p>

> import pandas as pd :: pandas Library
> import psycopg2 :: postgreSQL Library
> from connection import * :: bring over local variables from "connection.py"

<p>Retreive Cluster Properties</p>

> def prettyRedshiftProps(props):

<p>
    <ul>
        <li>Loads the Project Redshift Cluster Properties into the Dataframe.</li>
    </ul>
</p>

<p>Delete the Project Cluster and IAM Role</p>

> def deleteCluster(cur, conn):

<p>Create Connection & Execute functions:</p>

> def main():

<p>
    <ul>
        <li>conn = creates connection</li>
        <li>cur = creates the cursor</li>
        <li>drop_tables(cur,conn) = execute drop_tables function</li>
        <li>create_tables(cur,conn) = execute create_tables function</li>
    </ul>
</p>

<p>Launch Connection & Functions</p>

> if__name__ == "__main__": main()
