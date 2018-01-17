# AWS Glue Demo

This repository demonstrates how to use Python Boto3 library to access AWS Glue service and Identity and Access Management (IAM). 

## What does demo do?
First demo setups a managed resource policy that allows to read and write a user owned S3 resource. It also creates an IAM role and trust policy for Glue service to assume the role. Furthermore it attaches an AWS managed policy to the role created that grants the role access to Glue services.

As IAM policies and role are in place demo starts using actual Glue services. Demo first creates a Glue Crawler with above created role and a S3 location as a target. As Crawler is started it crawls all datasets found in the S3 folder and its subfolders. Glue service classifies the datasets and catalogues the data automatically. Demo then describes Glue generated database tables. 

As a final stage demo creates ETL jobs for both datasets and runs them. An ETL job exctracts a dataset in the data catalogue, transforms it and finally loads (stores) it in a S3 folder. Trasformation here is a simple format conversion from json csv (2. dataset). ETL generates as many output files as it found input files. By default Glue doesn't combine sources even with identical database schema.

## Datasets
Demo uses two different data sources:
1) Local Weather Data of Santa Rosa. Data format: csv
2) Traffic crash incident data of Town of Wary, North Carolina. Data format: json

Original datasets are available online here:
https://catalog.data.gov/dataset?res_format=CSV&tags=weather

Datasets have been truncated for demo purposes. Weather data (1) has been split in two two parts. Glue crawler automatically detects that parts have common database schema as long as the first rows are identical, i.e. all csv parts must contain table column names in the first row.

## Shortcomings
Demo assumes S3 bucket and required folders defined in demo_config.py are in place before running. TODO: create S3 resources dynamically.
The ETL scripts used in create_job() are genererated by AWS Glue. ETL jobs similar to those in this demo were created in Glue Console with same datasinks. It is possible to generate the scripts with Boto3. Scripts can only be run in Glue service due to depencies to low level AWS libraries.
