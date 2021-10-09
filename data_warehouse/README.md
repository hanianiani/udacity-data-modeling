#INTRO

This is an educational project as part of Udacity Data Engineering Nano Degree Program. All event logs are fictitious, 
and Sparkify is a made-up company.

# REDSHIFT-BASED DATA WAREHOUSE

## THE GOAL OF THE PROJECT

The aim of this project is to create a data warehouse based on Amazon Redshift and implement an ETL pipeline to extract 
data initially stored in AWS S3 buckets in JSON format, transforming the data into a STAR SCHEMA and loading it into 
Redshift, an AWS columnar database optimized for analytical aggregations.

## PREREQUISITES

In order to run this project you must create or have an AWS account and:
1. If you want to create the Redshift cluster programmatically using the scrip provided in `create_database.py` then 
include your AWS access key ID and AWS secret key in `dwh.cfg`. The mentioned script will handle the creation
of the Redshift cluster, IAM role and the database used in this project. 
*WARNING!! NEVER COMMIT YOUR AWS CREDENTIALS TO GIT OR PUBLISH OTHERWISE*
2. If you already have a Redshift cluster set up, then include the host name (endpoint) and the other database 
credentials in `dwh.cfg`.

## create_database.py
Running this script will create the following:
- Redshift cluster with Sparkify's database credentials, 
- IAM role, attaching the necessary policies.

If you do not want to create these from scratch and instead want to provide your own credentials you must modify the 
relevant attributes in `dwh.cfg`.

## sql_queries.py
This file contains the queries used in creating the staging tables where data is loaded from S3, the star schema tables 
and the INSERT queries which transform and load the data from the staging tables into the star schema.

## create_tables.py
Connects to the database and executes the creation of the staging and star schema tables.

## etl.py
Connects to the database and executes the loading and insertion queries.