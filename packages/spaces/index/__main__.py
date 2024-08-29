import boto3
import botocore.exceptions
import logging
import pprint
import sys
from botocore import UNSIGNED
from botocore.client import Config
import botocore.exceptions

# Move these into environment variables
SPACES_ENDPOINT = 'https://sfo3.digitaloceanspaces.com/'
REGION_NAME = 'sfo3'


class Bucket:
    def __init__(self, name, region_name=REGION_NAME, endpoint_url=SPACES_ENDPOINT):
        self._name = name
        self._region_name = region_name
        self._endpoint_url = endpoint_url
        self._contents = []
        self._filepaths = []
        self._session = boto3.session.Session()
        self._client = self._session.client('s3',
                                             region_name=self._region_name,
                                             endpoint_url=self._endpoint_url,
                                             config=Config(signature_version=UNSIGNED))
        
    @property
    def name(self):
        return self._name
    
    @property
    def contents(self):
        return self._contents
    
    @property
    def filepaths(self):
        return self._filepaths
    
    @property
    def base_bucket_url(self):
        return ''.join([self._endpoint_url, self._name])
    
    def fetch_bucket_names(self):
        bucket_names = []
        buckets = self._client.list_buckets()
        for bucket in buckets['Buckets']:
            bucket_names.append(bucket['Name'])
        return bucket_names
    
    def fetch(self):
        # Clear contents first
        self._contents = []
        self._filepaths = []
        total_count = 0
        logging.info('Fetching contents of bucket "%s" from %s', self._name, self._endpoint_url)
        paginator = self._client.get_paginator('list_objects_v2')
        pages = paginator.paginate(Bucket=self._name)

        for page in pages:
            iter_count = len(page.get('Contents'))
            total_count += iter_count
            logging.info('Retrieved %s objects', iter_count)
            self._contents.extend(page['Contents'])
            for obj in page['Contents']:
                self._filepaths.append(obj['Key'])
        logging.info('Retrieved total of %s objects, Done!', total_count)
    
    def make_html(self):
        filedict = {}
        for filepath in self.filepaths:
            try:
                path, filename = filepath.rsplit('/', 1)
            except ValueError:
                path = '/'
                filename = filepath
            filedict.setdefault(path, [])
            filedict[path].append(filename)

        html = '<html>\n<body>\n'
        html = '<html>\n<body>\n'
        directories = sorted(filedict.keys())
        for directory in directories:
            html += '<h2>' + directory + '</h2>\n<ul>\n'
            for filename in sorted(filedict[directory]):
                if directory == '/':
                    html += '<li><a href="' + '/'.join([self.base_bucket_url, filename]) + '">' + filename + '</a></li>\n'
                else:
                    html += '<li><a href="' + '/'.join([self.base_bucket_url, directory, filename]) + '">' + filename + '</a></li>\n'
            html += '</ul>\n'
        html += '</body>\n</html>\n'
        return html
        

def main(event, context):
    bucket_name = event.get('bucket')
    data = {
        'headers': {
            'Content-Type': 'text/html'
        },
        'statusCode': 404,
        'body': '<h1>404</h1><pre>No bucket specified or bucket not found (%s)<pre>' % bucket_name
    }

    if not bucket_name:
        logging.error('Bucket name (%s) not specified', bucket_name)
        return data

    b = Bucket(bucket_name)
    try:
        b.fetch()
    except botocore.exceptions.ClientError as e:
        #if e.response['Error']['Code'] == 'NoSuchBucket':
        logging.error('BUCKET DOES NOT EXIST: %s', bucket_name)
        return data
    data['statusCode'] = 200
    data['body'] = b.make_html()
    return data


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    pprint.pp(main({'bucket': sys.argv[1]}, None))