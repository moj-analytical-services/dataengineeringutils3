import os

import boto3
from moto import mock_s3, mock_sts
import pytest

from dataengineeringutils3.db import SelectQuerySet
from tests.helpers import mock_object
from tests.mocks import MockCursor


@pytest.fixture(scope="function")
def aws_credentials():
    """Mocked AWS Credentials for moto."""
    os.environ["AWS_ACCESS_KEY_ID"] = "testing"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
    os.environ["AWS_SECURITY_TOKEN"] = "testing"
    os.environ["AWS_SESSION_TOKEN"] = "testing"


@pytest.fixture(scope="function")
def s3(aws_credentials):
    with mock_s3():
        yield boto3.resource("s3", region_name="eu-west-1")


@pytest.fixture(scope="function")
def sts(aws_credentials):
    with mock_sts():
        yield boto3.resource("sts", region_name="eu-west-1")


@pytest.fixture
def select_queryset():
    return SelectQuerySet(mock_object(MockCursor, 15, [("Head 1",)]), "query", 2)


@pytest.fixture
def large_select_queryset():
    return SelectQuerySet(
        mock_object(MockCursor, 100000, [("Head 1",)]), "query", 10000
    )


@pytest.fixture
def result_set():
    return ['{"uuid": "fkjherpiutrgponfevpoir3qjgp8prueqhf9pq34hf89hwfpu92q"}'] * 100000
