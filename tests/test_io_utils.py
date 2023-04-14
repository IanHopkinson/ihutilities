#!/usr/bin/env python
# encoding: utf-8

import os
import unittest
import sys

from pathlib import Path
from unittest import TestCase

import pytest

from moto import mock_s3
from ihutilities.io_utils import (
    create_s3_client,
    iterator_from_filepath,
    file_handle_from_filepath,
    split_uri,
    join_paths_local_or_s3,
    write_dictionary,
    list_files_local_or_s3,
    expand_file_path,
)

from ihutilities import git_calculate_file_sha, calculate_file_sha

PP_REFERENCE_DATA_DIRECTORY = os.path.join(
    Path(__file__).parents[0], "fixtures", "land-registry-price-paid"
)

TEMP_FILE_PATH = os.path.join(Path(__file__).parents[0], "temp", "tmp.csv")
DICT_LIST = [
    {"a": 1, "b": 2, "c": 3},
    {"a": 4, "b": 5, "c": 6},
    {"a": 7, "b": 8, "c": 9},
]


@unittest.skip("Comparison between my implementation of sha and git sha has never worked")
class ShaCalculationTests(unittest.TestCase):
    def test_result_for_empty_file(self):
        test_root = os.path.dirname(__file__)
        filepath = os.path.join(test_root, "fixtures", "empty")
        self.assertEqual(git_calculate_file_sha(filepath), calculate_file_sha(filepath))

    def test_result_for_trivial_file(self):
        test_root = os.path.dirname(__file__)
        filepath = os.path.join(test_root, "fixtures", "sha_test_file")
        self.assertEqual(git_calculate_file_sha(filepath), calculate_file_sha(filepath))

    def test_result_for_zip_content(self):
        test_root = os.path.dirname(__file__)
        zip_path = os.path.join(test_root, "fixtures", "survey_csv.zip/survey_csv.csv")
        norm_path = os.path.join(test_root, "fixtures", "survey_csv.csv")
        self.assertEqual(calculate_file_sha(norm_path), calculate_file_sha(zip_path))

    def test_result_for_larger_file(self):
        test_root = os.path.dirname(__file__)
        norm_path = os.path.join(test_root, "fixtures", "survey_csv.csv")
        self.assertEqual(git_calculate_file_sha(norm_path), calculate_file_sha(norm_path))


def test_write_dictionary_raises_an_index_error():
    TestCase().assertRaises(IndexError, write_dictionary, TEMP_FILE_PATH, [])


def test_write_dictionary_raises_a_key_error():
    TestCase().assertRaises(AttributeError, write_dictionary, TEMP_FILE_PATH, [0, 1, 2])


def test_write_dictionary_to_local_file():
    if os.path.isfile(TEMP_FILE_PATH):
        os.remove(TEMP_FILE_PATH)

    status = write_dictionary(TEMP_FILE_PATH, DICT_LIST)
    rows_read = list(iterator_from_filepath(TEMP_FILE_PATH))

    TestCase().assertEqual(len(rows_read), 3)
    TestCase().assertDictEqual(rows_read[0], {"a": "1", "b": "2", "c": "3"})
    TestCase().assertIn("New file", status)
    TestCase().assertIn("is being created", status)


@mock_s3
def test_write_dictionary_to_s3():
    filename = "tmp.csv"
    s3_uri = f"s3://test-bucket/{filename}"
    s3_bucket, _, _ = split_uri(s3_uri)
    s3_client = create_s3_client()

    bucket_name = s3_bucket.replace("s3://", "")
    s3_client.create_bucket(
        Bucket=bucket_name, CreateBucketConfiguration={"LocationConstraint": "eu-west-2"}
    )

    objs = list(s3_client.list_objects(Bucket=bucket_name, Prefix=filename))
    if "Contents" in objs:
        s3_client.delete_object(Bucket=bucket_name, Key=filename)

    status = write_dictionary(s3_uri, DICT_LIST)
    rows_read = list(iterator_from_filepath(s3_uri))

    TestCase().assertEqual(len(rows_read), 3)
    TestCase().assertDictEqual(rows_read[0], {"a": "1", "b": "2", "c": "3"})
    TestCase().assertEqual(status, "New file s3://test-bucket/tmp.csv is being created")


@pytest.mark.skipif(
    not sys.platform.startswith("win"),
    reason="Skipping local file directory split for Azure Devops since Devops is on Ubuntu",
)
def test_split_uri_local_file():
    local_uri = "C:\\temp\\folder\\filename.csv"
    directory, key, filename = split_uri(local_uri)

    TestCase().assertEqual(directory, "C:\\temp\\folder")
    TestCase().assertEqual(key, "")
    TestCase().assertEqual(filename, "filename.csv")


def test_split_uri_s3():
    s3_uri = "s3://gbgdmsp-staging-land-registry-house-price-index/2021-03-01/Indices-2021-03.csv"
    bucket, key, filename = split_uri(s3_uri)

    TestCase().assertEqual(bucket, "s3://gbgdmsp-staging-land-registry-house-price-index")
    TestCase().assertEqual(key, "2021-03-01/Indices-2021-03.csv")
    TestCase().assertEqual(filename, "Indices-2021-03.csv")


def test_split_uri_works_on_a_bucket_only_s3_uri():
    local_uri = "s3://gbgdmsp-staging-land-registry-house-price-index"
    bucket, key, filename = split_uri(local_uri)

    TestCase().assertEqual(bucket, "s3://gbgdmsp-staging-land-registry-house-price-index")
    TestCase().assertEqual(key, "")
    TestCase().assertEqual(filename, "")


@mock_s3
def test_file_handle_from_filepath_raises_file_not_found_on_s3(make_s3_test_files):
    s3_file_uri = "s3://gbgdmsp-staging-land-registry-price-paid/wrong_file.md"
    bucket_name = "gbgdmsp-staging-land-registry-price-paid"
    file_list = [
        (
            "documentation.md",
            os.path.join(PP_REFERENCE_DATA_DIRECTORY, "documentation.md"),
        )
    ]

    make_s3_test_files(
        bucket_name,
        file_list,
    )

    TestCase().assertRaisesRegex(
        FileNotFoundError,
        "Bucket name: gbgdmsp-staging-land-registry-price-paid with key: wrong_file.md not found",
        file_handle_from_filepath,
        s3_file_uri,
    )


def test_list_files_local():
    file_directory = os.path.join(Path(__file__).parents[0])

    files = list_files_local_or_s3(f"{file_directory}/*.py")

    TestCase().assertEqual(len(files), 10)


@mock_s3
def test_list_files_s3_with_pattern(make_s3_test_files):
    s3_pattern = "s3://gbgdmsp-staging-land-registry-price-paid/*/documentation.md"
    bucket_name = "gbgdmsp-staging-land-registry-price-paid"
    source_file = os.path.join(PP_REFERENCE_DATA_DIRECTORY, "documentation.md")
    file_list = [
        (
            "2022-09-22/documentation.md",
            source_file,
        ),
        (
            "2022-08-22/documentation.md",
            source_file,
        ),
        (
            "2022-07-22/documentation.md",
            source_file,
        ),
    ]

    make_s3_test_files(
        bucket_name,
        file_list,
    )

    files = list_files_local_or_s3(s3_pattern)

    TestCase().assertEqual(len(files), 3)


@mock_s3
def test_list_files_s3_for_subdirectories(make_s3_test_files):
    s3_pattern = "s3://gbgdmsp-staging-land-registry-price-paid/*/"  # trailing / for directory
    bucket_name = "gbgdmsp-staging-land-registry-price-paid"
    source_file = os.path.join(PP_REFERENCE_DATA_DIRECTORY, "documentation.md")
    subdirectories = ["2022-07-22", "2022-08-22", "2022-09-22"]
    file_list = [(x + "/documentation.md", source_file) for x in subdirectories]

    make_s3_test_files(
        bucket_name,
        file_list,
    )

    files = list_files_local_or_s3(s3_pattern)

    TestCase().assertEqual(
        files,
        [
            "s3://gbgdmsp-staging-land-registry-price-paid/2022-07-22",
            "s3://gbgdmsp-staging-land-registry-price-paid/2022-08-22",
            "s3://gbgdmsp-staging-land-registry-price-paid/2022-09-22",
        ],
    )


def test_join_paths_local_or_s3():
    measured_s3_path = join_paths_local_or_s3(
        "s3://some-bucket-name", "2020-09-05", "attributes.csv"
    )
    expected_s3_path = "s3://some-bucket-name/2020-09-05/attributes.csv"
    TestCase().assertEqual(measured_s3_path, expected_s3_path)

    measured_s3_path_2 = join_paths_local_or_s3("s3://some-bucket-name", "*/")
    expected_s3_path_2 = "s3://some-bucket-name/*/"
    TestCase().assertEqual(measured_s3_path_2, expected_s3_path_2)

    measured_s3_path = join_paths_local_or_s3(
        "s3://some-bucket-name", "2020-09-05/", "attributes.csv"
    )
    expected_s3_path = "s3://some-bucket-name/2020-09-05/attributes.csv"
    TestCase().assertEqual(measured_s3_path, expected_s3_path)


def test_expand_file_path_makes_a_path_absolute():
    pattern = "tests/fixtures/*.csv"
    example_path_full = expand_file_path(pattern)
    TestCase().assertEqual(example_path_full, os.path.join(os.getcwd(), pattern))


def test_expand_file_path_expands_a_list_of_files():
    pattern = "tests/fixtures/land-registry-price-paid/*.md"
    full_pattern = expand_file_path(pattern)
    example_files = list_files_local_or_s3(full_pattern)
    TestCase().assertEqual(len(example_files), 1)
