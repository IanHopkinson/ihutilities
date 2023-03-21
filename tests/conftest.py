#!/usr/bin/env python
# encoding: utf-8

import os

import boto3
import pytest
from moto import mock_s3

os.environ["AWS_ACCESS_KEY_ID"] = "testing"
os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
os.environ["AWS_SECURITY_TOKEN"] = "testing"
os.environ["AWS_SESSION_TOKEN"] = "testing"


fixtures_directory = os.path.join(
    os.path.abspath(os.path.dirname(__file__)),
    "fixtures",
)


@pytest.fixture()
def make_s3_test_files():
    @mock_s3
    def _make_s3_test_files(bucket_name, file_list):
        session = boto3.Session()
        s3_session = session.resource("s3")
        bucket_name = bucket_name.replace("s3://", "")
        s3_session.create_bucket(
            Bucket=bucket_name, CreateBucketConfiguration={"LocationConstraint": "eu-west-2"}
        )

        for key, source_filepath in file_list:
            s3_target = s3_session.Object(bucket_name, key)
            _ = s3_target.put(Body=open(source_filepath, "rb"))

    return _make_s3_test_files
