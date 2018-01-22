# AWS Glue Demo

AWS Glue Demo is a Python application that demonstrates how to use AWS SDK for Python (Boto3 library) to access AWS Glue, Simple Object Storage (S3) and Identity and Access Management (IAM) services. 
AWS Glue is a fully managed extract, transform, and load (ETL) service that makes it easy for customers to prepare and load their data for analytics. 

https://aws.amazon.com/glue/

## Prerequisites
### Runnning the demo requires: 
1) Python (python-2.7 is recommended).
2) Boto3 AWS SDK library installed
3) AWS API authentication tokens configured in development environment, e.g. with AWS command-line interface (aws configure).

Detailed setup instuctions can be found e.g. here:
https://docs.aws.amazon.com/glue/latest/dg/aws-glue-programming-python-setup.html

## How to run the demo

Clone source code to a convenient location in your development environment and execute. 
```
$ git clone https://github.com/mtakanen/aws-glue-demo .
$ cd glue-demo/src
$ python glue_demo.py
```

Demo is non-interactive. Progress and some interesting information is printed in stdout (terminal console). Some of the API calls to AWS are asynchronous. Script always waits for service processes to terminate as following stages depend on the previous results.

## Datasets
Demo uses two different data sources:
1) Local Weather Data of Santa Rosa. Data format: csv
2) Traffic crash incident data of Town of Wary, North Carolina. Data format: json

Original datasets are available online here:
https://catalog.data.gov/dataset?res_format=CSV&tags=weather

Datasets have been truncated for demo purposes. Weather data sample (1) is stored in two parts to simulate split data files often necessary when working with large datasets. Glue crawler automatically detects parts that share a common database schema, as long as they are found in same folder and the first rows are identical, i.e. all csv file parts must contain table column names in the first row.

## What does demo do?
Application first setups a managed resource policy that allows to read and write a user owned S3 resource. It also creates an IAM role and trust policy for Glue service to assume the role. Furthermore, an AWS managed policy is attached to the role that grants access to Glue services. 

Application also setups data resources in S3. Bucket named DEMO_BUCKET_NAME (refer demo_config.py) is created and input datafiles are uploaded in folders as follows:
```
DEMO_BUCKET_NAME/data 
DEMO_BUCKET_NAME/data/input
DEMO_BUCKET_NAME/data/input/incidents
DEMO_BUCKET_NAME/data/input/weather
DEMO_BUCKET_NAME/data/output
DEMO_BUCKET_NAME/etl-srcipts
```

As IAM policies and data resources are in place we start using actual Glue services. First we create a Glue Crawler with the role above and a S3 location as a target. Next we request to start the Crawler in the Glue service. Crawler progresses through all datasets in the S3 target folder (DEMO_BUCKET_NAME/data/input) and its subfolders. Glue service automatically classifies the datasets and catalogues them in a database. Application then requests the Glue generated database tables and describes their schemas.

As a final stage we create ETL jobs for both datasets and run them. Each ETL job exctracts a dataset in the data catalogue, transforms it and finally loads the actual data in a S3 folder. We make simple trasformations: For dataset (1) ETL script requests a column type change, and for dataset (2) file format is conversed from json to csv. By default Glue generates as many output files as it has found input files.

## Shortcomings
The ETL scripts used in create_job() are actually genererated by AWS Glue service. ETL jobs similar to those in this demo were created in Glue Console with the same datasinks as demo uses. According to documentation it is possible to generate the ETL scripts with Boto3. Note 1: ETL scripts can only be run inside the Glue service due to dependencies to low level AWS libraries. Note 2: If DEMO_BUCKET_NAME is changed, connection_options = {"path": "s3://DEMO_BUCKET_NAME/data/output" in ETL scripts must be edited accordingly.
