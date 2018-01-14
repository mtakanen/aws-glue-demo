import boto3

DEFAULT_REGION='eu-west-1'
DEFAULT_GLUE_ENDPOINT='glue'
DEFAULT_SERVICE_ROLE='arn:aws:iam::051356523739:role/service-role/AWSGlueServiceRole-DefaultRole'
DEFAULT_CRAWLER_NAME='demo-crawler'
DEFAULT_S3_INPUT_PATH='s3://glue-demo-mtakanen/data/input'

def create_crawler(glue_client, crawler_name, crawler_description, aws_role, 
                   db_name, s3_target_path):

    print 'create_crawler()'

    try:
        response = glue_client.create_crawler(
            Name=crawler_name,
            Role=aws_role,
            DatabaseName=db_name,
            Description=crawler_description,
            Targets = {
                'S3Targets': [
                    {
                        'Path': s3_target_path
                    }
                ]
            },
            TablePrefix=db_name,
            SchemaChangePolicy={
                'UpdateBehavior': 'UPDATE_IN_DATABASE',
                'DeleteBehavior': 'DEPRECATE_IN_DATABASE'
            }       
        )
        return response
    except glue_client.exceptions.AlreadyExistsException as e:
        print 'Crawler "%s" already exists. Returning existing crawler.' %crawler_name
        return glue_client.get_crawler(Name=crawler_name)

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
    # boto3 doesn't have built-in waiters for glue.
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

    while state != exit_state:
        time.sleep(1)
        response = client.get_crawler(Name=crawler_name)
        state = response.get('Crawler').get('State')
        if state == prev_state:
            print '.'
        else:
            print state
            prev_state = state


def main():
    glue_endpoint = DEFAULT_GLUE_ENDPOINT
    region = DEFAULT_REGION
    role = DEFAULT_SERVICE_ROLE
    crawler_name = DEFAULT_CRAWLER_NAME
    s3_input_path = DEFAULT_S3_INPUT_PATH

    glue = boto3.client(service_name='glue', region_name=region,
                          endpoint_url='https://%s.%s.amazonaws.com' 
                          %(glue_endpoint, region))

    
    response = create_crawler(glue_client=glue,
                              crawler_name=crawler_name,
                              crawler_description='Crawler for demo data',
                              aws_role=role,
                              db_name='demo',
                              s3_target_path=s3_input_path)
    start_crawler(glue, crawler_name) 
    # start_crawler is asyncronous -> wait until crawler is in ready state
    wait_crawler(glue, crawler_name, 'READY')

if __name__ == '__main__':
    main()
         
