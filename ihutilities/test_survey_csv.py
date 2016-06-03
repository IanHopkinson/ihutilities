#!/usr/bin/env python
# encoding: utf-8

from ihutilities.survey_csv import survey_csv

def test_line_count():
    # file_path = "N:\\2016\\code\\simple-and-open\\logs\\accuracy.csv"
    file_path = "ihutilities\\fixtures\\survey_csv.csv"
    survey_csv(file_path)