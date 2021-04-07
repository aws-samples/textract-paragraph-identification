# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0

import boto3

textract_client = boto3.client("textract")
comprehend = boto3.client("comprehend")

'''
This method updates textract-job-details DynamoDB table to store the information about job run history.
'''
def update_metadata_with_status(job_id, document_path, job_status, completed_time):
    job_status_table = boto3.resource('dynamodb').Table("textract-job-details")
    job_status_table.update_item(
        Key={
            'file_path': document_path,
            'job_id': job_id
        },
        UpdateExpression="SET job_status = :job_status, completed_time = :timestamp",
        ExpressionAttributeValues={
            ':job_status': job_status, ':timestamp': completed_time}
    )


'''
This method is used for page with Headers and Text with same left indentation but different font.

This method takes text metadata collected and identifies different font sizes.
Based on the size of the font, it separates the text. 

Once the text is separated, it uses font size to identify headers at different levels.
Filters out all the headers that does not have any text after them.



'''

def get_headers_to_child_mapping(font_sizes_and_line_numbers):
    unique_font_heights = []
    for font_height in font_sizes_and_line_numbers.keys():
        lines_with_same_font = font_sizes_and_line_numbers[font_height]
        if len(lines_with_same_font) > 1:
            unique_font_heights.append(font_height)

    fonts_for_headers = list(set(unique_font_heights))
    i = 0
    headers_and_its_child = {}
    while i + 1 < len(fonts_for_headers):
        headers_and_its_child[fonts_for_headers[i]] = fonts_for_headers[i + 1]
        i += 1
    return headers_and_its_child


'''
This method takes job id generated at the time of Textract call as input
Textracts provides one page response per call and API will be called repeatedly until there are no more pages.
This returns complete text collected by Textract as response.
'''
def get_text_results_from_textract(job_id):
    response = textract_client.get_document_text_detection(JobId=job_id)
    collection_of_textract_responses = []
    pages = [response]

    collection_of_textract_responses.append(response)

    while 'NextToken' in response:
        next_token = response['NextToken']
        response = textract_client.get_document_text_detection(
            JobId=job_id, NextToken=next_token)
        pages.append(response)
        collection_of_textract_responses.append(response)
    return collection_of_textract_responses


'''
This method takes the complete textract response as an input and iterates through all the pages and collects Lines and required bounding box info.
Once the lines are extracted, a running sequence number is used to set the line numbers.
The line numbers, left indentation information, indentation from top and font size will be extracted from Textract raw response.
'''

def get_the_text_with_required_info(collection_of_textract_responses):
    total_text = []
    total_text_with_info = []
    running_sequence_number = 0

    font_sizes_and_line_numbers = {}
    for page in collection_of_textract_responses:
        per_page_text = []
        blocks = page['Blocks']
        for block in blocks:
            if block['BlockType'] == 'LINE':
                block_text_dict = {}
                running_sequence_number += 1
                block_text_dict.update(text=block['Text'])
                block_text_dict.update(page=block['Page'])
                block_text_dict.update(left_indent=round(
                    block['Geometry']['BoundingBox']['Left'], 2))
                font_height = round(
                    block['Geometry']['BoundingBox']['Height'], 3)
                line_number = running_sequence_number
                block_text_dict.update(font_height=round(
                    block['Geometry']['BoundingBox']['Height'], 3))
                block_text_dict.update(line_number=running_sequence_number)

                if font_height in font_sizes_and_line_numbers:
                    line_numbers = font_sizes_and_line_numbers[font_height]
                    line_numbers.append(line_number)
                    font_sizes_and_line_numbers[font_height] = line_numbers
                else:
                    line_numbers = []
                    line_numbers.append(line_number)
                    font_sizes_and_line_numbers[font_height] = line_numbers

                total_text.append(block['Text'])
                per_page_text.append(block['Text'])
                total_text_with_info.append(block_text_dict)

    return total_text_with_info, font_sizes_and_line_numbers


'''
This method takes the headers to paragraphs information and runs sentiment analysis using Comprehend.
The headers and their corresponding paragraph text with sentiment will be stored in DynamoDB table.
'''
def update_paragraphs_info_in_dynamodb(headers_to_paragraphs, document_path):
    textract_post_process_table = boto3.resource(
        'dynamodb').Table("textract-post-process-data")
    for identified_header in headers_to_paragraphs.keys():
        print("inserting data for {}".format(identified_header))
        paragraph_sentiment = comprehend.detect_sentiment(
            Text=headers_to_paragraphs[identified_header],
            LanguageCode='en'
        )
        textract_post_process_table.put_item(
            Item={'file_path': document_path,
                  'paragraph_header': identified_header,
                  'paragraph_data': headers_to_paragraphs[identified_header],
                  'paragraph_sentiment': paragraph_sentiment['Sentiment']
                  })
