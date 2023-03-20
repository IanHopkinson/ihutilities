#!/usr/bin/env python
# encoding: utf-8

import warnings
from unittest import TestCase

import pytest

from ihutilities.misc_utils import get_with_alts


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
