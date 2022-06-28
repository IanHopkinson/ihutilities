#!/usr/bin/env python
# encoding: utf-8

import os
from ihutilities.survey_csv import survey_csv


def test_line_count():
    test_root = os.path.dirname(__file__)
    # file_path = "N:\\2016\\code\\simple-and-open\\logs\\accuracy.csv"
    file_path = os.path.join(test_root, "fixtures", "survey_csv.csv")
    survey_csv(file_path)
