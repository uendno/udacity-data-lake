import configparser
import os
import shutil

from etl import run

# config = configparser.ConfigParser()
# config.read_file(open('/Users/uendno/.aws/credentials'))

# os.environ['AWS_REGION'] = 'us-east-1'
# os.environ['AWS_ACCESS_KEY_ID'] = config.get('default', 'AWS_ACCESS_KEY_ID')
# os.environ['AWS_SECRET_ACCESS_KEY'] = config.get('default', 'AWS_SECRET_ACCESS_KEY')

run('s3a://udacity-dend/song_data/*/*/*/*.json', 's3a://udacity-dend/log_data/*.json', 's3a://gotit-data-lake')
