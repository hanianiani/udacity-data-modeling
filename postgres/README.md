**Disclaimer:** This is an educational project within the scope of the Udacity Data Engineering nanoDegree program. Sparkify is a fictional company and the songplays and user data is simulated. 

# The Goal of the Project

This project creates a PostgreSQL database by implementing an ETL procedure on .json log files.

The resulting database simplifies the querying process for the analytics team of Sparkify, a fictional music streaming platform.

The analytics team of Sparkify is in particular interested to investigate what songs are popular with users and correlations such as timeframe, location, user level (paid vs free), etc.

The final database consists of the following tables:
* artists
* songs
* users
* songplays
* time

Songplays is the primary facts table containing records about each individual songplay event. By joining this table to the provided dimensional tables it is possible to perform various analytical aggregations.

# ETL process description 

All the tables are created by extracting and transforming the data contained in the folder named (very imaginatively =)) *data*. This folder contains two sets of log files:
1. Log files containing metadata on *songs* and *artists*
2. Log files containing information about the user activity on the Sparkify app

The project also contains:
* Three python scripts which perform the actual ETL process:
    * sql_queries.py - the queries used in creating the mentioned five tables and inserting the pre-processed data. There is no need to run this script when testing the project;
    * create_tables.py - a python script creating the database using the create_table queries from the sql_queries.py file. This script should be run first;
    * etl.py - a python script which runs transformations on the log files and then using the insert queries from sql_queries.py loads the resulting records into the created tables;
* etl.ipynb - a Jupyter notebook used in the development process: running this file is not mandatory but it explains the step-by-step development process;
* test.ipynb - after running the two scripts you can use this notebook to test the result.

In etl.py we use two functions to transform the data:

* process_song_file processes a single metadata file and splits and inserts the relevant attributes into two tables: *songs* and *artists*;
* process_log_file processes a single sonplay event file and runs some more transformational processes in order to fetch and insert data for *time*, *users* and *songplays* tables;

In process_log_file where feasible data is pre-cleaned in order to reduce the workload on the database.

Since the underlying data is a fraction of a larger dataset, there are very few foreign key matches, therefore no foreign key constraints are imposed.