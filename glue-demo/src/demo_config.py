DEFAULT_REGION='eu-west-1'
DEFAULT_BUCKET_NAME='glue-demo-mtakanen' #FIXME -> DEMO_BUCKET_NAME
GLUE_ENDPOINT='glue'
DATABASE_NAME='demo'
CRAWLER_NAME='demo-crawler'
DEMO_ROLE_NAME='GlueDemoRole'
DEMO_POLICY_NAME='GlueDemoPolicy'

# AWS managed policy for Glue services
AWS_GLUE_SERVICE_POLICY='arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole'

# S3 locations used
DATA_INPUT_PATH='s3://'+DEFAULT_BUCKET_NAME+'/data/input'
ETL_SCRIPT_DIR='s3://'+DEFAULT_BUCKET_NAME+'/etl-scripts'
S3_TEMP_DIR='s3://'+DEFAULT_BUCKET_NAME+'/tmp'

# ETL job names
JOB_NAME_INCIDENTS='demo-job-incidents'
JOB_NAME_WEATHER='demo-job-weather'

# Glue generated ETL scripts
ETL_SCRIPT_INCIDENTS='demo-etl-incidents.py'
ETL_SCRIPT_WEATHER='demo-etl-weather.py'

# The magic command for glue is documented here: 
# https://docs.aws.amazon.com/glue/latest/webapi/API_JobCommand.html
ETL_COMMAND_NAME='glueetl' 

# https://docs.aws.amazon.com/glue/latest/dg/aws-glue-api-jobs-runs.html
MIN_DPU_CAPACITY=2

# magic delay to allow IAM role to propagate other services
IAM_PROPAGATION_DELAY=9

# interval in seconds for polling service state
POLL_INTERVAL=3
