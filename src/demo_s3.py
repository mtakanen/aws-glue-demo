import os
import sys
import boto3
import unittest

from demo_config import *

"""Contains utils for S3 related demo setup. 
USAGE:
if not bucket_exists(bucket_name):
    setup_s3(bucket_name)
else:
   print "bucket_name must be unique in region."
"""

def create_bucket(client, bucket_name):
    s3 = boto3.resource('s3', region_name=DEFAULT_REGION)
    s3.create_bucket(ACL='private', Bucket=bucket_name, 
                         CreateBucketConfiguration=
                         {
                             'LocationConstraint':DEFAULT_REGION
                         })

def bucket_exists(s3_client, bucket_name):
    try:
        resp = s3_client.head_bucket(Bucket=bucket_name)
        return True
    except s3_client.exceptions.ClientError as e:
        return False

def put_object(client, bucket, key):
    client.put_object(ACL='private', Bucket=bucket, Key=key)

def upload_file(s3_resource, bucket, local_path, s3_path):
    s3_resource.Bucket(bucket).upload_file(local_path, s3_path)

def setup_s3(s3_client, bucket_name):
    print 'setup_s3()'
    script_path = os.path.abspath(os.path.dirname(sys.argv[0]))
    project_root = os.path.split(script_path)[0]
   
    create_bucket(s3_client, bucket_name)

    s3 = boto3.resource('s3')

    #upload input data
    local_datapath = os.path.join(project_root, 'data/')
    for fname in os.listdir(local_datapath):
        if 'weather' in fname:
            s3_datapath = 'data/input/weather'
        elif 'incident' in fname:
            s3_datapath = 'data/input/incidents'
        upload_file(s3, bucket_name, 
                    os.path.join(local_datapath, fname), 
                    os.path.join(s3_datapath, fname))

    # create output folder
    s3_client.put_object(ACL='private', Bucket=bucket_name, Key=DATA_OUTPUT_FOLDER)

    #upload etl scripts
    local_script_path = os.path.join(project_root, 'etl-scripts')
    s3_datapath = 'etl-scripts'
    for fname in os.listdir(local_script_path):
        upload_file(s3, bucket_name, 
                    os.path.join(local_script_path, fname), 
                    os.path.join(s3_datapath, fname))

def list_folder_keys(s3_client, bucket_name, path_prefix):
    # first delete contents of bucket
    keys = []
    response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=path_prefix)
    for c in response.get('Contents'):
        if not c.get('Key').endswith(path_prefix): # skips folder name            
            keys.append(c.get('Key'))

    return keys

def delete_bucket_contents(s3_client, bucket_name):
    # first delete contents of bucket
    response = s3_client.list_objects_v2(Bucket=bucket_name)
    for obj in response.get('Contents') or []:
        s3_client.delete_object(Bucket=bucket_name, Key=obj.get('Key'))
        
    s3_client.delete_bucket(Bucket=bucket_name)


class Test_demo_s3(unittest.TestCase):
    import uuid

    TEST_BUCKET_NAME='test-bucket-%s' %str(uuid.uuid4())
    TEST_FILENAME='README.md'

    def setUp(self):
        self.client = boto3.client('s3')


    def tearDown(self):
        # first delete contents of bucket
        response = self.client.list_objects_v2(Bucket=self.TEST_BUCKET_NAME)
        for obj in response.get('Contents') or []:
            self.client.delete_object(Bucket=self.TEST_BUCKET_NAME, Key=obj.get('Key'))

        self.client.delete_bucket(Bucket=self.TEST_BUCKET_NAME)

    def test_create_bucket(self):
        create_bucket(self.client, self.TEST_BUCKET_NAME)
        try:
            resp = self.client.head_bucket(Bucket=self.TEST_BUCKET_NAME)
        except self.client.exceptions.ClientError as e:
            self.fail(e)

    def test_bucket_exists(self):
        self.assertFalse(bucket_exists(self.client, self.TEST_BUCKET_NAME))
        create_bucket(self.client, self.TEST_BUCKET_NAME)
        self.assertTrue(bucket_exists(self.client, self.TEST_BUCKET_NAME))

    def test_put_object(self):
        create_bucket(self.client, self.TEST_BUCKET_NAME)
        put_object(self.client, self.TEST_BUCKET_NAME, 'folder_name/')


    def test_upload_file(self):
        s3_folder = ''
        script_path = os.path.abspath(os.path.dirname(sys.argv[0]))
        project_root = os.path.split(script_path)[0]
        local_path = os.path.join(project_root, self.TEST_FILENAME)
        s3_path = os.path.join(s3_folder, self.TEST_FILENAME)

        create_bucket(self.client, self.TEST_BUCKET_NAME)

        s3 = boto3.resource('s3')
        upload_file(s3, self.TEST_BUCKET_NAME, local_path, s3_path)

        # test that file exists in s3
        try:
            response = self.client.get_object(Bucket=self.TEST_BUCKET_NAME, Key=s3_path)
        except self.client.exceptions.ClientError as e:
            self.fail(e)

            
    def test_setup_s3(self):

        script_path = os.path.abspath(os.path.dirname(sys.argv[0]))
        project_root = os.path.split(script_path)[0]
        
        setup_s3(self.client, self.TEST_BUCKET_NAME)

        #test input data
        local_datapath = os.path.join(project_root, 'data')
        s3_datapath = 'data/input/'
        for fname in os.listdir(local_datapath):
            if 'weather' in fname:
                s3_datapath = 'data/input/weather'
            elif 'incident' in fname:
                s3_datapath = 'data/input/incidents'
            try:
                response = self.client.get_object(Bucket=self.TEST_BUCKET_NAME, 
                                                  Key=os.path.join(s3_datapath, fname))
            except self.client.exceptions.ClientError as e:
                self.fail("%s (%s)" %(e,fname))

        '''
        # can't get empty folder !
        try:
            response = self.client.get_object(Bucket=self.TEST_BUCKET_NAME, 
                                              Key=os.path.join(s3_datapath, 'data/output/'))
        except self.client.exceptions.ClientError as e:
            self.fail("%s (%s)" %(e,'data/output/'))
        '''

        #test etl scripts
        local_datapath = os.path.join(project_root, 'etl-scripts')
        s3_datapath = 'etl-scripts'
        for fname in os.listdir(local_datapath):
            try:
                response = self.client.get_object(Bucket=self.TEST_BUCKET_NAME, 
                                                  Key=os.path.join(s3_datapath, fname))
            except self.client.exceptions.ClientError as e:
                self.fail("%s (%s)" %(e,fname))

    def test_list_folder_contents(self):

        create_bucket(self.client, self.TEST_BUCKET_NAME)
        put_object(self.client, self.TEST_BUCKET_NAME, 'folder_name1/filename1')
        put_object(self.client, self.TEST_BUCKET_NAME, 'folder_name2/filename2')
        put_object(self.client, self.TEST_BUCKET_NAME, 'folder_name2/filename3')
        path_prefix = 'folder_name2'

        keys = list_folder_keys(self.client, self.TEST_BUCKET_NAME, path_prefix)
        for key in keys:
           self.assertTrue(key.startswith(path_prefix))
           self.assertFalse(key.endswith(path_prefix))

if __name__ == "__main__":
    unittest.main()

        
