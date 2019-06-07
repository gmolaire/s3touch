import boto3
import argparse
import base64
import gzip
import hashlib
import logging
import itertools
import os
import time
import json
from datetime import datetime
import time

import pprint

pp = pprint.PrettyPrinter(indent=2)

parser = argparse.ArgumentParser(description='Touch a S3 Bucket files by updating their metadata.')
parser.add_argument('-c', '--contains', help='A string that should be part of the S3 object to touch.', default='')
parser.add_argument('-d', '--delay', help='Delay between each touch in second.', default=0.5, type=float)
parser.add_argument('-b', '--bucket', help='The S3 Bucket to touch.', required=True)


args = parser.parse_args()
bucket = args.bucket
containing = args.contains
post_copy_delay = args.delay


# Set the logging config
logging.basicConfig(format='%(asctime)s - %(levelname)s - %(message)s', level=logging.INFO)


s3_client = boto3.client('s3')
touched_them_all = False
next_marker = ''

# Go through each pageto touch the contents representing the files in the bucket
while not touched_them_all:
    objects = s3_client.list_objects(
        Bucket=bucket,
        MaxKeys=500,
        Marker=next_marker
    )

    # Only pursue when it's paged, AKA truncated
    if objects['IsTruncated']:
        if 'NextMarker' in objects:
            next_marker = objects['NextMarker']
        else:
            next_marker = objects['Contents'][-1]['Key']
    else:
        touched_them_all = True

    # Let's touch each objects
    for object in objects['Contents']:
        if containing in object['Key']:
            response = s3_client.copy_object(
                Bucket=bucket,
                CopySource={'Bucket': bucket, 'Key': object['Key']},
                Key=object['Key'],
                Metadata={
                    'TouchedAt': str(datetime.now())
                },
                MetadataDirective='REPLACE'
            )

            logging.info(f'Touched {object["Key"]} at {response["CopyObjectResult"]["LastModified"]}')

            # Sleep a bit
            time.sleep(post_copy_delay)