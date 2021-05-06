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

    required_mapping = lambda_helper.get_headers_to_child_mapping(font_sizes_and_line_numbers)

    header_and_its_line_numbers = get_headers_and_their_line_numbers(required_mapping,total_text_with_info)
    headers_to_paragraphs = get_headers_and_paragraphs(header_and_its_line_numbers,
                                                       total_text_with_info)

    lambda_helper.update_paragraphs_info_in_dynamodb(
        headers_to_paragraphs,
        document_path)

    return json.dumps(headers_to_paragraphs)

def get_headers_and_their_line_numbers(required_mapping, total_text_with_info):
    header_and_its_line_numbers = {}
    for header in required_mapping.keys():
        for each_line in total_text_with_info:
            if each_line['font_height'] == header:
                header_and_its_line_numbers[each_line['text']] = each_line['line_number']
    return header_and_its_line_numbers


def get_headers_and_paragraphs(header_and_its_line_numbers,
                               total_text_with_info):
    headers_to_paragraphs = {}
    header_list_iterator = iter(header_and_its_line_numbers)
    header = next(header_list_iterator, None)

    while header:
        header_line_number = header_and_its_line_numbers[header]
        current_header = header
        header = next(header_list_iterator, None)
        paragraph_data = []
        if header:
            next_header_line_number = header_and_its_line_numbers[header]
            for each_line in total_text_with_info:
                if (each_line['line_number'] > header_line_number) and (
                        each_line['line_number'] < next_header_line_number):
                    paragraph_data.append(each_line['text'])
        else:
            for each_line in total_text_with_info:
                if each_line['line_number'] > header_line_number:
                    paragraph_data.append(each_line['text'])

        headers_to_paragraphs[current_header] = " ".join(paragraph_data)
    return headers_to_paragraphs
