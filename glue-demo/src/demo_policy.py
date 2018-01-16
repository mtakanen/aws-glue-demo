import boto3
import json
from demo_config import *


def create_policy(iam_client):
    print 'create_policy'
    policy_arn=''
    s3_resource_arn="arn:aws:s3:::%s/*" %DEFAULT_BUCKET_NAME

    demo_policy = {
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
                "Resource": s3_resource_arn
            }
        ]
    }

    try:
        response = iam_client.create_policy(
            PolicyName='GlueDemoPolicy',
            PolicyDocument=json.dumps(demo_policy))
        policy_arn = response.get('Policy').get('Arn')
    except iam.exceptions.EntityAlreadyExistsException as e:
        print e
    except iam.exceptions.MalformedPolicyDocumentException as e:
        print e
        
    return policy_arn

def create_role(iam_client, role_name):
    print 'create_role'
    trust_policy = {
        "Version": "2012-10-17",
        "Statement": {
            "Effect": "Allow",
            "Principal": {"Service": "glue.amazonaws.com"},
            "Action": "sts:AssumeRole"
        }
    }

    try:
        response = iam_client.create_role(
            RoleName=role_name,
            AssumeRolePolicyDocument=json.dumps(trust_policy))
    except iam.exceptions.EntityAlreadyExistsException as e:
        print e
    except iam.exceptions.MalformedPolicyDocumentException as e:
        print e

def delete_demo_role_policy():

    iam_client = boto3.client('iam')
    #FIXME: find dynamically, which api?
    policy_arn = 'arn:aws:iam::051356523739:policy/GlueDemoPolicy'
   
    try:
        iam_client.detach_role_policy(
            PolicyArn=policy_arn,
            RoleName=DEMO_ROLE_NAME)
        iam_client.delete_policy(PolicyArn=policy_arn)
        iam_client.delete_role(RoleName=DEMO_ROLE_NAME)
        print 'demo role and policy deleted.'
    except iam_client.exceptions.NoSuchEntityException as e:
        print e

def create_demo_role_policy():
    iam_client = boto3.client('iam')
    policy_arn = create_policy(iam_client)
    create_role(iam_client, DEMO_ROLE_NAME)
    if policy_arn:
        print 'attach policy to role'
        iam_client.attach_role_policy(PolicyArn=policy_arn, 
                                      RoleName=DEMO_ROLE_NAME)
    else:
        print 'Unable to attach policy to the role. Invalid PolicyArn: %s' %policy_arn



