#!/usr/bin/env python
# encoding: utf-8

import os
import unittest

from ihutilities import git_calculate_file_sha, calculate_file_sha

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