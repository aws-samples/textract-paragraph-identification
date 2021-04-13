# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0

import json
import boto3
from datetime import datetime
import lambda_helper

textract_client = boto3.client("textract")
comprehend = boto3.client("comprehend")

def lambda_handler(event, context):
    notification_message = json.loads(event['Records'][0]['Sns']['Message'])

    job_id = notification_message['JobId']
    document_path = "s3://{}/{}".format(notification_message['DocumentLocation']['S3Bucket'],
                                        notification_message['DocumentLocation']['S3ObjectName'])

    job_status = notification_message['Status']
    completed_time_stamp = notification_message['Timestamp'] / 1000
    completed_time = datetime.fromtimestamp(completed_time_stamp).strftime('%Y-%m-%d %H:%M:%S')
    lambda_helper.update_metadata_with_status(job_id,document_path,job_status,completed_time)

    collection_of_textract_responses = lambda_helper.get_text_results_from_textract(job_id)

    total_text_with_info, font_sizes_and_line_numbers = lambda_helper.get_the_text_with_required_info(
        collection_of_textract_responses)

    text_with_line_spacing_info = lambda_helper.get_text_with_line_spacing_info(total_text_with_info)
    paragraphs = lambda_helper.extract_paragraphs_only(text_with_line_spacing_info)
    response = {'paragraphs':paragraphs}
    return json.dumps(response)