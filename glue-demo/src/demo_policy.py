import boto3
import json
from demo_config import *

S3_RESOURCE_ARN="arn:aws:s3:::%s/*" %DEFAULT_BUCKET_NAME

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

def create_policy(iam_client, policy_name):
    print 'create_policy'
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
    print 'create_role'

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


def delete_demo_role_policy():

    iam_client = boto3.client('iam')
    #FIXME: find dynamically, which api?
    policy_arn = 'arn:aws:iam::051356523739:policy/AWSGlueServiceRole-GlueDemoRole'
   
    try:
        iam_client.detach_role_policy(PolicyArn=policy_arn,
                                      RoleName=DEMO_ROLE_NAME)
        iam_client.detach_role_policy(PolicyArn=AWS_GLUE_SERVICE_POLICY, 
                                      RoleName=DEMO_ROLE_NAME)

        iam_client.delete_policy(PolicyArn=policy_arn)
        iam_client.delete_role(RoleName=DEMO_ROLE_NAME)
        print 'demo role and policy deleted.'
    except iam_client.exceptions.NoSuchEntityException as e:
        print e

def create_demo_role_policy(session):

    iam_client = session.client('iam')
    policy_arn = create_policy(iam_client, DEMO_POLICY_NAME)
    role_arn = create_role(iam_client, DEMO_ROLE_NAME)

    #inline role policy, !work
    #iam_client.put_role_policy(RoleName=DEMO_ROLE_NAME, 
    #                           PolicyName=DEMO_POLICY_NAME,
    #                           PolicyDocument=json.dumps(RESOURCE_POLICY))

    # attach AWS managed policy for Glue
    iam_client.attach_role_policy(PolicyArn=AWS_GLUE_SERVICE_POLICY, 
                                  RoleName=DEMO_ROLE_NAME)
    
    if policy_arn:
        print 'attach policy to role'
        iam_client.attach_role_policy(PolicyArn=policy_arn, 
                                      RoleName=DEMO_ROLE_NAME)
    else:
        print 'Unable to attach policy to the role. Invalid PolicyArn: %s' %policy_arn
    
    return role_arn

