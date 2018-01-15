import boto3

DEFAULT_REGION='eu-west-1'
DEFAULT_BUCKET='s3://glue-demo-mtakanen'
DEFAULT_SERVICE_ROLE='arn:aws:iam::051356523739:role/service-role/AWSGlueServiceRole-DefaultRole'

GLUE_ENDPOINT='glue'
S3_INPUT_PATH=DEFAULT_BUCKET+'/data/input'
DATABASE_NAME='demo'
CRAWLER_NAME='demo-crawler'

JOB_NAME_INCIDENTS='demo-job-incidents'
ETL_COMMAND_NAME_INCIDENTS='demo-etl-incidents'
ETL_SCRIPT_DIR=DEFAULT_BUCKET+'/etl-scripts'
S3_TEMP_DIR=DEFAULT_BUCKET+'/tmp'
MIN_DPU_CAPACITY=2

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
        print response
    except client.exceptions.AlreadyExistsException as e:
        print 'Crawler "%s" already exists. Use existing crawler!' %crawler_name

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

def wait_crawler(client, crawler_name, exit_state):
    # boto3 doesn't have built-in waiters for glue crawler.
    # poll crawler status
    import time

    print 'wait_crawler()'
    try:
        response = client.get_crawler(Name=crawler_name)
    except glue_client.exceptions.EntityNotFoundException as e:
        print 'Nothing to wait. Crawler %s does not exist.' %crawler_name
        return

    prev_state=''
    state = response.get('Crawler').get('State')
    print 'Initial crawler state: %s' %state

    while state != exit_state:
        time.sleep(3)
        response = client.get_crawler(Name=crawler_name)
        state = response.get('Crawler').get('State')
        if state == prev_state:
            print '.'
        else:
            print state
            prev_state = state

    print 'Crawler is in wait exit state: %s' %exit_state

def show_tables(client, db_name):

    print 'show_tables()'
    print '\tDatabase: %s' %db_name
    try:
        response = client.get_tables(DatabaseName=db_name)
    except client.exceptions.EntityNotFoundException as e:
        print e
        return

    tables = response.get('TableList')
    for table in tables:
        print 'Name: %s' %table['Name']
        print 'Classification: %s' %table.get('Parameters').get('classification')
        storage_desc = table.get('StorageDescriptor')
        print 'Location: %s' %storage_desc.get('Location')

        print 'Serde params: %s' %repr(storage_desc.get('SerdeInfo').get('Parameters'))
        print 'Last updated: %s' %table.get('UpdateTime')
        print ''
        print 'Schema'
        print 'Column name\tData type'
        for column in storage_desc.get('Columns'):
            row = '%s\t%s' %(column['Name'], column['Type'])
            print row

        print '\n'



def create_job(client, job_name, job_description, aws_role, 
               etl_command_name, etl_script_location):
    print 'create_job()'
    try:
        resp = client.create_job(Name=job_name, 
                                 Description=job_description,
                                 Role=aws_role,
                                 Command=
                                 {    
                                     'Name':etl_command_name, 
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
        print '%s created.'%resp['Name']
    except Exception as e:
        print e

def main():
    glue_endpoint = GLUE_ENDPOINT
    region = DEFAULT_REGION
    role = DEFAULT_SERVICE_ROLE
    crawler_name = CRAWLER_NAME
    s3_input_path = S3_INPUT_PATH
    db_name = DATABASE_NAME

    glue_client = boto3.client(service_name='glue', region_name=region,
                               endpoint_url='https://%s.%s.amazonaws.com' 
                               %(glue_endpoint, region))
    
    create_crawler(client=glue_client,
                   crawler_name=crawler_name,
                   crawler_description='Crawler for demo data',
                   aws_role=role,
                   db_name=db_name,
                   s3_target_path=s3_input_path)

    start_crawler(glue_client, crawler_name) # FIXME: save pennies, assumes db_name exists in Glue DataCatalogue
    # start_crawler is asyncronous -> wait until crawler is in ready state
    wait_crawler(glue_client, crawler_name, 'READY')
    show_tables(glue_client, db_name)

    job_name = JOB_NAME_INCIDENTS
    etl_command_name = ETL_COMMAND_NAME_INCIDENTS
    etl_script_location = ETL_SCRIPT_DIR+'/'+ETL_COMMAND_NAME_INCIDENTS+'.py'

    create_job(client=glue_client, 
               job_name=job_name,
               job_description='A job for incidents ETL.',
               aws_role=role,
               etl_command_name=etl_command_name,
               etl_script_location=etl_script_location)
    
if __name__ == '__main__':
    main()
         
