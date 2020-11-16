from bs4 import BeautifulSoup
import scraperwiki

from datetime import datetime
import pickle
import re
import sys
import unicodedata

import boto3
from botocore.exceptions import ClientError

# AWS Credentials
ACCESS_KEY = ''
SECRET_KEY = ''
with open('./aws_key.csv') as f:
    for line in f.readlines():
        line = line.strip()
        access_match = re.search(r'^AWSAccessKeyId=(.+)$', line)
        secret_match = re.search(r'^AWSSecretKey=(.+)$', line)

        if access_match:
            ACCESS_KEY = access_match.group(1)

        if secret_match:
            SECRET_KEY = secret_match.group(1)

dynamodb = boto3.resource('dynamodb', aws_access_key_id=ACCESS_KEY, aws_secret_access_key=SECRET_KEY, region_name='us-west-2')

table_name = 'Jeopardy_Clues'

table = dynamodb.Table(table_name)
print('Loaded Table')

