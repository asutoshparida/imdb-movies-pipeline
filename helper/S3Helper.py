"""
S3Helper.py
~~~~~~~~

Module containing helper function for connecting to S3
"""
import boto3


class S3Helper(object):

    @staticmethod
    def read_s3_file(bucket_name, file_to_read, aws_access_id, aws_secret_key, region_name='us-east-1'):
        s3client = boto3.client('s3', aws_access_key_id=aws_access_id, aws_secret_access_key=aws_secret_key,
                                region_name=region_name)
        file_obj = s3client.get_object(
            Bucket=bucket_name,
            Key=file_to_read
        )
        # open the file object and read it into the variable file_data.
        file_data = file_obj['Body'].read()

        # file data will be a binary stream.  We have to decode it
        file_contents = file_data.decode('utf-8')
        return file_contents

    @staticmethod
    def split_s3_path(s3_path):
        path_parts = s3_path.replace("s3://", "").split("/")
        bucket = path_parts.pop(0)
        key = "/".join(path_parts)
        print(bucket)
        print(key)
        return bucket, key

