#!/usr/bin/env python
# encoding: utf-8

from nose.tools import assert_equal, assert_greater
from ihutilities import git_sha, git_uncommitted_changes

def test_git_sha():
    sha = git_sha(".")
    print(type(sha))
    assert_equal(isinstance(sha, str), True)
    assert_greater(len(sha), 5)

def test_git_uncommitted():
    is_uncommitted = git_uncommitted_changes("",".")
    assert_equal(isinstance(is_uncommitted, bool), True)