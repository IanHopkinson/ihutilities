#!/usr/bin/env python
# encoding: utf-8

import unittest
from ihutilities import git_sha, git_uncommitted_changes


class GitTests(unittest.TestCase):
    def test_git_sha(self):
        sha = git_sha(".")
        self.assertEqual(isinstance(sha, str), True)
        self.assertGreater(len(sha), 5)

    def test_git_uncommitted(self):
        is_uncommitted = git_uncommitted_changes("", ".")
        self.assertEqual(isinstance(is_uncommitted, bool), True)
