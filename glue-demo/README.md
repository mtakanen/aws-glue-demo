# AWS Glue Demo

This repository demonstrates how to use Python Boto3 library to access AWS Glue service. It also demostates how to use AWS Identity and Access Management (IAM) and Simpl Storage Service (S3). 

## Data
Demo uses two different data sources:
1) Local Weather Data of Santa Rosa. Data format: csv
2) Traffic crash incident data of Town of Wary, North Carolina. Data format: json

Original datasets are available online here:
https://catalog.data.gov/dataset?res_format=CSV&tags=weather

Datasets have been truncated for demo purposes. Weather data (1) has been split in two parts. Glue crawler automatically detects that parts belong to same dataset as long as the first rows are identical, i.e. all csv parts must contain table column names on the first row.
 
