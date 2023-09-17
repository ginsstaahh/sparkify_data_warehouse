# Intro
This project is a program used for the latest and greatest music app created by Sparkify that acts as a backend framework to store, insert, and provide you with songs, artist information, and albums to the AWS Redshift data warehouse.  It uses an ETL pipeline to transfer data from S3 into Redshift.  The program provides the essential information about Sparkify users' interests used for BI data analytics.

There are five tables in the Sparkify database used for data analytics: songplays, users, time, songs, and artists.  Each database table contain their own data columns and primary keys using int, float, varchar, text, and bigint data types.  Songplays table differs from other tables because it includes data from two foreign tables (songs and artists).  Two other tables exist used for staging: staging_events, staging_songs.

## Table of Contents
- [Database Schema](#database-schema)
- [Files in Repository](#files-in-repository)
- [Prerequisites](#prerequisites)
- [Running Python Scripts](#running-python-scripts)
- [Using Redshift on AWS](#using-redshift-on-aws)
- [Code Style](#code-style)
- [Contribution Guidelines](#contribution-guidelines)
- [License and Acknowledgements](#license-and-acknowledgements)

## Database Schema
Here is a diagram based off of the star schema that gives an overview of the tables and their data columns
![alt text](/sparkify_erd.png?raw=true)

The star schema shows several tables including:
songplays: a fact table that records log data associated with song plays
users: a dimension table containing data of the users of the app
songs: a dimension table containing data of songs in the database
artists: a dimension table containing artists in the database
time: a dimension table containing timestamps of records in songplays broken down into specific units

## Files in Repository
dwh.cfg - configuration file specifying information related to the Redshift cluster as well as AWS credentials

sql_queries.py - creates and provides all the SQL used for providing database table structure, deletion code and insertion statements
create_tables.py - used as middleware that takes SQL code from sql_queries.py for the purposes of creation, deletion, as well as insertion of the database used and its tables.
etl.py - contains the logic to process data insertion to Redshift.

sparkify_erd.png - star schema diagram of fact and dimension tables found in the project

## Prerequisites
Check to see if your UNIX system has already has python by using the command in your terminal:
python --version

Python can be installed for ubuntu or debian linux using the command:
sudo apt-get install python

## Running Python Scripts
A terminal can be used to run the python files to generate tables.  Using the terminal, which can be found in the launcher tab, run create_tables.py in the root working directory to create the backend framework needed to assess the working code:
python create_tables.py

You can now use etl.py to insert data:
python etl.py

## Using Redshift on AWS
After running create_tables.py, the cluster should be found on AWS Redshift as such:
![alt text](/sparkify_cluster.png?raw=true)

After performing an ETL, you can use the query editor in the AWS Redshift console to retrieve data from tables:
![alt text](/query_editor.png?raw=true)

## Code Style
Coding style follows PEP8 style guidelines