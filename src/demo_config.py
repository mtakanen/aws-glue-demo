import datetime
"""Demo configuration. Contains constants used in the demo."""

DEFAULT_REGION='eu-west-1'
DEMO_BUCKET_NAME='glue-demo-bucket-%s' %str(datetime.datetime.now().date()) + '-' + str(datetime.datetime.now().time()).replace(':', '').replace('.','')
GLUE_ENDPOINT='glue'
DATABASE_NAME='demo'
CRAWLER_NAME='demo-crawler'
DEMO_ROLE_NAME='GlueDemoRole'
DEMO_POLICY_NAME='GlueDemoPolicy'

# AWS managed policy for Glue services
AWS_GLUE_SERVICE_POLICY='arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole'

# S3 locations used
DATA_INPUT_PATH='s3://'+DEMO_BUCKET_NAME+'/data/input'
DATA_OUTPUT_FOLDER='data/output/'
ETL_SCRIPT_DIR='s3://'+DEMO_BUCKET_NAME+'/etl-scripts'
S3_TEMP_DIR='s3://'+DEMO_BUCKET_NAME+'/tmp'

# ETL job names
JOB_NAME_INCIDENTS='demo_job_incidents'
JOB_NAME_WEATHER='demo_job_weather'

# Glue generated ETL scripts
ETL_SCRIPT_INCIDENTS='demo_etl_incidents.py'
ETL_SCRIPT_WEATHER='demo_etl_weather.py'

# The magic command for glue is documented here: 
# https://docs.aws.amazon.com/glue/latest/webapi/API_JobCommand.html
ETL_COMMAND_NAME='glueetl' 

# https://docs.aws.amazon.com/glue/latest/dg/aws-glue-api-jobs-runs.html
MIN_DPU_CAPACITY=2

# interval in seconds for polling service state
POLL_INTERVAL=3
