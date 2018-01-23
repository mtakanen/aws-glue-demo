import boto3
import json
import unittest
from demo_config import *

"""Setups role and policy required by demo. Contains AWS IAM related functions."""

S3_RESOURCE_ARN="arn:aws:s3:::%s/*" %DEMO_BUCKET_NAME

RESOURCE_POLICY = {
    # "Description": "Grants access to a S3 bucket.",
    # raises MalformedPolicyDocumentException!
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Action": [
                "s3:PutObject",
                "s3:GetObject"
             ],
            "Resource": S3_RESOURCE_ARN
        }
    ]
}

TRUST_POLICY = {
    "Version": "2012-10-17",
    "Statement": {
        "Effect": "Allow",
        "Principal": {
            "Service": "glue.amazonaws.com"
        },
        "Action": [
            "sts:AssumeRole"
        ]
    }
}

def create_managed_policy(iam_client, policy_name):

    print 'create_managed_policy()'
    policy_arn=''
    
    try:
        response = iam_client.create_policy(
            PolicyName=policy_name,
            PolicyDocument=json.dumps(RESOURCE_POLICY))
        policy_arn = response.get('Policy').get('Arn')
    except iam_client.exceptions.EntityAlreadyExistsException as e:
        print e
    except iam_client.exceptions.MalformedPolicyDocumentException as e:
        print e
        
    return policy_arn

def create_role(iam_client, role_name):
    print 'create_role()'

    try:
        response = iam_client.create_role(
            Path='/service-role/',
            RoleName=role_name,
            AssumeRolePolicyDocument=json.dumps(TRUST_POLICY))
        return response.get('Role').get('Arn')
    except iam_client.exceptions.EntityAlreadyExistsException as e:
        print e
    except iam_client.exceptions.MalformedPolicyDocumentException as e:
        print e

def create_role_inline_policy(iam_client):

    create_role(iam_client, DEMO_ROLE_NAME)
    iam_client.put_role_policy(RoleName=DEMO_ROLE_NAME, 
                               PolicyName=DEMO_POLICY_NAME,
                               PolicyDocument=json.dumps(RESOURCE_POLICY))

def delete_role_and_policy(client, role_name, policy_name):

    policy_arn = ''
    response = client.list_policies(Scope='Local')
    for policy in response.get('Policies'):
        if policy.get('PolicyName') == policy_name:
            policy_arn = policy.get('Arn')
            break

    if policy_arn == '':
        # managed policy not found, try to delete role
        try:
            client.delete_role(RoleName=role_name)
        except client.exceptions.NoSuchEntityException as e:
            pass
        return

    try:
        client.detach_role_policy(PolicyArn=policy_arn,
                                  RoleName=role_name)
        client.detach_role_policy(PolicyArn=AWS_GLUE_SERVICE_POLICY, 
                                  RoleName=role_name)

        client.delete_policy(PolicyArn=policy_arn)
        client.delete_role(RoleName=role_name)
        print 'demo role and policy deleted.'
    except client.exceptions.NoSuchEntityException as e:
        pass

def create_demo_role_policy(client):

    policy_arn = create_managed_policy(client, DEMO_POLICY_NAME)
    role_arn = create_role(client, DEMO_ROLE_NAME)

    # attach AWS managed policy for Glue
    client.attach_role_policy(PolicyArn=AWS_GLUE_SERVICE_POLICY, 
                              RoleName=DEMO_ROLE_NAME)
    
    # attach customer managed policy
    if policy_arn:
        client.attach_role_policy(PolicyArn=policy_arn, 
                                  RoleName=DEMO_ROLE_NAME)
    else:
        print 'Unable to attach policy to the role. Invalid PolicyArn: %s' %policy_arn
    
    return role_arn

def get_or_create_demo_role_policy(client):
    try:
        response = client.get_role(RoleName=DEMO_ROLE_NAME)
        return response.get('Role').get('Arn')
    except client.exceptions.NoSuchEntityException as e:
        return create_demo_role_policy(client)
        

class Test_demo_policy(unittest.TestCase):

    TEST_POLICY_NAME='TestPolicy'    
    TEST_ROLE_NAME='TestRole'

    def setUp(self):
        self.client = boto3.client('iam')

    def tearDown(self):

        response = self.client.list_policies(Scope='Local')
        for policy in response.get('Policies'):
            if policy.get('PolicyName') == self.TEST_POLICY_NAME:
                self.client.delete_policy(PolicyArn=policy.get('Arn'))

        try:
            self.client.delete_role(RoleName=self.TEST_ROLE_NAME)
        except self.client.exceptions.NoSuchEntityException as e:
            pass

    def test_create_managed_policy(self):
        policy_arn = create_managed_policy(self.client, self.TEST_POLICY_NAME)
        assert policy_arn != ''

    def test_create_managed_policy_exists(self):
        policy_name = self.TEST_POLICY_NAME
        create_managed_policy(self.client, policy_name)
        policy_arn = create_managed_policy(self.client, policy_name)
        assert policy_arn == ''

    def test_create_role(self):
        role_arn = create_role(self.client, self.TEST_ROLE_NAME)
        response = self.client.get_role(RoleName=self.TEST_ROLE_NAME)
        assert role_arn == response.get('Role').get('Arn')

    def test_delete_role_and_policy(self):
        delete_role_and_policy(self.client, DEMO_ROLE_NAME, DEMO_POLICY_NAME)
        with self.assertRaises(self.client.exceptions.NoSuchEntityException):
            response = self.client.get_role(RoleName=self.TEST_ROLE_NAME)

        response = self.client.list_policies(Scope='Local')
        for policy in response.get('Policies'):
            self.assertNotEqual(policy.get('PolicyName'), self.TEST_POLICY_NAME)



if __name__ == "__main__":
    unittest.main()
