# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0

import json
import boto3
import uuid
from datetime import datetime
import os

def lambda_handler(event, context):
    textract_client = boto3.client('textract')
    textract_job_details_table = boto3.resource('dynamodb').Table("textract-job-details")
    sns_arn = os.environ['SNSTOPIC']
    iam_arn = os.environ['IAMARN']

    ## Get the object name
    bucket = event['Records'][0]['s3']['bucket']['name']
    key = event['Records'][0]['s3']['object']['key']

    ## ClientRequestToken should be unique when calling textract
    unique_hash_for_client_request_token = uuid.uuid4().hex
    ## Invoke textract start analysis and store the record in DynamoDB
    textract_invocation_response = textract_client.start_document_text_detection(
        DocumentLocation={
            'S3Object': {
                'Bucket': bucket,
                'Name': key
            }
        },
        ClientRequestToken=unique_hash_for_client_request_token,
        NotificationChannel={
            'SNSTopicArn': sns_arn,
            'RoleArn': iam_arn
        })

    print("textract document text detection completed. Job id is {} and path is s://{}".format(textract_invocation_response['JobId'],
                                                                                               bucket + key))
    db_response = textract_job_details_table.put_item(
        Item={'file_path': "s3://{}/{}".format(bucket, key),
              'job_id': textract_invocation_response['JobId'],
              'job_status': 'SUBMITTED',
              'submission_time': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
              })

    print("record has been inserted into dynamodb table {}".format("textract-job-details"))

    
    return {
        'statusCode': 200,
        'body': json.dumps(db_response)
    }
