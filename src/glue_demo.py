import boto3
import time
import sys, os
import re
import unittest

import demo_policy
import demo_s3
from demo_config import *

"""Main script of the demo. Contains AWS Glue related functions."""

def create_crawler(client, crawler_name, crawler_description, aws_role, 
                   db_name, s3_target_path):

    print 'create_crawler()'

    try:
        response = client.create_crawler(
            Name=crawler_name,
            Description=crawler_description,
            Role=aws_role,
            Targets = {
                'S3Targets': [
                    {
                        'Path': s3_target_path
                    }
                ]
            },
            DatabaseName=db_name,
            TablePrefix=db_name+'_',
            SchemaChangePolicy=
            {
                'UpdateBehavior': 'UPDATE_IN_DATABASE',
                'DeleteBehavior': 'DEPRECATE_IN_DATABASE'
            }       
        )
        print 'Created crawler %s.' %crawler_name
        return 'SUCCESS'
    except client.exceptions.AlreadyExistsException as e:
        print 'Crawler "%s" already exists. Use existing crawler!' %crawler_name
    except client.exceptions.InvalidInputException as e:
        # IAM role has not propagated to Glue service
        print e
        return 'FAILED'


def start_crawler(glue_client,crawler_name):
    print 'start_crawler()'
    try:
        glue_client.start_crawler(Name=crawler_name)
    except glue_client.exceptions.EntityNotFoundException as e:
        print e
    except glue_client.exceptions.CrawlerRunningException as e:
        print e
    except glue_client.exceptions.OperationTimeoutException as e:
        print e


def poll_crawler_state(client, crawler_name):
    try:
        response = client.get_crawler(Name=crawler_name)
        return response.get('Crawler').get('State')
    except glue_client.exceptions.EntityNotFoundException as e:
        print 'Crawler not found (%s).' %crawler_name

def poll_job_run_state(client, job_name, run_id):
    try:
        jr = client.get_job_run(JobName=job_name, RunId=run_id)
        return jr.get('JobRun').get('JobRunState')
    except client.exceptions.EntityNotFoundException as e:
        print 'Job run not found (RunId:%s).' %run_id


def show_tables(client, db_name):

    print 'show_tables()'
    print '\nDatabase: %s' %db_name
    try:
        response = client.get_tables(DatabaseName=db_name)
    except client.exceptions.EntityNotFoundException as e:
        print e
        return

    tables = response.get('TableList')
    for table in tables:
        print 'Table name: %s' %table['Name']
        print 'Classification: %s' %table.get('Parameters').get('classification')
        print 'Record count: %s' %table.get('Parameters').get('recordCount')
        print 'Avg. record size: %s' %table.get('Parameters').get('averageRecordSize')

        storage_desc = table.get('StorageDescriptor')
        print 'Location: %s' %storage_desc.get('Location')
        print 'Serde params: %s' %repr(storage_desc.get('SerdeInfo').get('Parameters'))
        print 'Last updated: %s' %table.get('UpdateTime')

        print '\nSchema'
        print 'Column name\tData type'
        for column in storage_desc.get('Columns'):
            row = '%s\t%s' %(column['Name'], column['Type'])
            print row

        print '\n'



def create_job(client, job_name, job_description, aws_role, 
               etl_script_location):

    print 'create_job()'
    try:
        resp = client.create_job(Name=job_name, 
                                 Description=job_description,
                                 Role=aws_role,
                                 Command=
                                 {    
                                     'Name' : ETL_COMMAND_NAME, 
                                     'ScriptLocation': etl_script_location
                                 }, 
                                 DefaultArguments= 
                                 {
                                     '--TempDir': S3_TEMP_DIR,
                                     '--job-language': 'python',
                                     '--job-bookmark-option': 'job-bookmark-disable'
                                 },
                                 AllocatedCapacity=MIN_DPU_CAPACITY, 
                                 MaxRetries=0)
        print 'Created job %s.'%resp['Name']
    except client.exceptions.AlreadyExistsException as e:
        print e
    except client.exceptions.ConcurrentModificationException as e:
        print e
    except client.exceptions.IdempotentParameterMismatchException as e:
        print e
    except client.exceptions.InternalServiceException as e:
        print e
    except client.exceptions.InvalidInputException as e:
        print e
    except client.exceptions.OperationTimeoutException as e:
        print e
    except client.exceptions.ResourceNumberLimitExceededException as e:
        print e

def start_job_run(client, job_name):

    print 'run_job()'
    run_id = -1

    try:
        resp = client.start_job_run(JobName=job_name,
                                    AllocatedCapacity=MIN_DPU_CAPACITY)
        run_id = resp.get('JobRunId')
        print 'Job run started'
        print 'JobRunId:%s' %run_id

    except client.exceptions.ConcurrentRunsExceededException as e:
        print e
    except client.exceptions.EntityNotFoundException as e:
        print e
    except client.exceptions.InternalServiceException as e:
        print e
    except client.exceptions.InvalidInputException as e:
        print e
    except client.exceptions.OperationTimeoutException as e:
        print e
    except client.exceptions.ResourceNumberLimitExceededException as e:
        print e

    return run_id

def create_and_run_etl_job(client, job_name, job_description, etl_script_location):
    script_location = ETL_SCRIPT_DIR+'/'+etl_script_location
    create_job(client=client, 
               job_name=job_name,
               job_description=job_description,
               aws_role=DEMO_ROLE_NAME,
               etl_script_location=script_location)

    exit_pattern = re.compile('(?!RUNNING)') # matches anything but "RUNNING"
    run_id = start_job_run(client=client, job_name=job_name)
    wait_state(exit_pattern, poll_job_run_state, 
               client, job_name, run_id)


def wait_state(exit_pattern, poll_function, *args):
    """Waiter function for asynchronous service calls as Boto3 doesn't provide built-in waiters 
    for Glue. Polls the status of a Glue process with poll_function passed as an argument."""

    prev_state=''
    while True:
        state = poll_function(*args)
        if not state:
            break
        if state != prev_state:
            prev_state = state
            print('\n%s' %state), # discard newline, (python2.x only)
            sys.stdout.flush() 
        else:
            print('.'),
            sys.stdout.flush()
        if exit_pattern.match(state):
            print ''
            break        

        time.sleep(POLL_INTERVAL)

def main():
    """Demo application main function. Setups AWS dependencies and sequences demo steps."""
    print '*** AWS Glue Demo by @mtakanen ***'

    iam_client = boto3.client('iam', region_name=DEFAULT_REGION)
    role_arn = demo_policy.get_or_create_demo_role_policy(iam_client)

    # NOTE: It may take a while as newly created IAM role/policy propagates to other services.
    # https://aws.amazon.com/premiumsupport/knowledge-center/assume-role-validate-listeners/

    s3_client = boto3.client('s3', region_name=DEFAULT_REGION)
    
    if not demo_s3.bucket_exists(s3_client, DEMO_BUCKET_NAME):
        demo_s3.setup_s3(s3_client, DEMO_BUCKET_NAME)
    else:
        print 'FATAL: bucket (%s) exists in region (%s).' %(DEMO_BUCKET_NAME, DEFAULT_REGION)
        exit(1)

    glue_client = boto3.client(service_name='glue', 
                               region_name=DEFAULT_REGION,
                               endpoint_url='https://%s.%s.amazonaws.com' 
                               %(GLUE_ENDPOINT, DEFAULT_REGION))


    exit_pattern = re.compile('SUCCESS')
    wait_state(exit_pattern, create_crawler,
               glue_client,
               CRAWLER_NAME,
               'Glue demo crawler',
               role_arn,
               DATABASE_NAME,
               DATA_INPUT_PATH)

    start_crawler(glue_client, CRAWLER_NAME) 

    # start_crawler() is asyncronous -> wait until crawler is not "RUNNING"
    exit_pattern = p=re.compile('(?!RUNNING)')
    wait_state(exit_pattern, poll_crawler_state, 
               glue_client, CRAWLER_NAME)

    show_tables(glue_client, DATABASE_NAME)

    create_and_run_etl_job(client=glue_client,
                           job_name=JOB_NAME_WEATHER,
                           job_description='A job for weather ETL.',
                           etl_script_location=ETL_SCRIPT_WEATHER)

    create_and_run_etl_job(client=glue_client, 
                           job_name=JOB_NAME_INCIDENTS,
                           job_description='A job for incidents ETL.',
                           etl_script_location=ETL_SCRIPT_INCIDENTS)

    print 'Output files in S3:'
    output_keys = demo_s3.list_folder_keys(s3_client, DEMO_BUCKET_NAME, DATA_OUTPUT_FOLDER)
    for key in output_keys:
        print "%s/%s" %(DEMO_BUCKET_NAME, key)

class Test_glue_demo(unittest.TestCase):
    """Unittests for glue_demo module.
    USAGE:
    python -m unittest glue_demo.Test_glue_demo """

    import time

    def setUp(self):
        pass

    def tearDown(self):
        pass

    def test_wait_state_ready(self):
        def poll_func(response):
            return response
        
        exit_pattern = re.compile('READY')
        wait_state(exit_pattern, poll_func, 'READY')

    def test_wait_state_until_time(self):
        def poll_time(until_time):
            if until_time > time.time():
                return 'RUNNING'
            else:
                return 'SUCCESS'

        until_time = time.time() + 2 * POLL_INTERVAL
        exit_pattern = re.compile('SUCCESS')
        wait_state(exit_pattern, poll_time, until_time)

if __name__ == '__main__':
    main()
         
