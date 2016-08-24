#!/usr/bin/env python
# encoding: utf-8

import unittest

from ihutilities import git_calculate_file_sha, calculate_file_sha

class ShaCalculationTests(unittest.TestCase):
    def test_result_for_empty_file(self):
        filepath = "fixtures/empty"
        self.assertEqual(git_calculate_file_sha(filepath), calculate_file_sha(filepath))
    
    def test_result_for_trivial_file(self):
        filepath = "fixtures/sha_test_file"
        self.assertEqual(git_calculate_file_sha(filepath), calculate_file_sha(filepath)) 