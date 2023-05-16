#!/usr/bin/env python
# encoding: utf-8

import logging
import os
import warnings
from unittest import TestCase

import pytest

from ihutilities.misc_utils import get_with_alts, initialise_logger


def test_get_with_alts_with_primary_key():
    test_dictionary = {"dataset_name": "success"}
    value = get_with_alts(test_dictionary, "dataset_name")
    TestCase().assertEqual(value, "success")


def test_get_with_alts_with_alt_key():
    test_dictionary = {"Identifier": "success"}

    with warnings.catch_warnings(record=True) as captured_warnings:
        value = get_with_alts(test_dictionary, "dataset_name", alternatives=["Identifier"])

    TestCase().assertEqual(len(captured_warnings), 1)
    TestCase().assertTrue(issubclass(captured_warnings[0].category, DeprecationWarning))
    TestCase().assertEqual(
        str(captured_warnings[0].message),
        ("as of version 2022.12.5 'Identifier' is replaced with 'dataset_name'"),
    )
    TestCase().assertEqual(value, "success")


def test_get_with_alts_with_variant_key():
    test_dictionary = {"Identifier": "success"}
    with warnings.catch_warnings(record=True) as captured_warnings:
        value = get_with_alts(test_dictionary, "identifier")

    TestCase().assertEqual(len(captured_warnings), 1)
    TestCase().assertTrue(issubclass(captured_warnings[0].category, DeprecationWarning))
    TestCase().assertEqual(
        str(captured_warnings[0].message),
        ("as of version 2022.12.5 'Identifier' is replaced with 'identifier'"),
    )

    TestCase().assertEqual(value, "success")


def test_get_with_alts_with_absent_key(capfd):
    test_dictionary = {"Identifier": "success"}
    with pytest.raises(KeyError):
        _ = get_with_alts(test_dictionary, "dave")

    printed, _ = capfd.readouterr()
    TestCase().assertEqual(printed, "")


def test_initialise_logging(caplog):
    logname = os.path.basename(__file__).replace(".py", "")
    logfile_path = os.path.join("tests", "temp", "{}.log".format(logname))

    if os.path.exists(logfile_path):
        os.remove(logfile_path)
    initialise_logger(logfile_path, mode="both")  # Supports modes "both" and "file only"

    logging.info("Test message")
    TestCase().assertIn("INFO     root:test_misc_utils.py:", caplog.text)
    TestCase().assertIn("Test message", caplog.text)
    TestCase().assertTrue(os.path.exists(logfile_path))
