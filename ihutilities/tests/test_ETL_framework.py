#!/usr/bin/env python
# encoding, utf-8

import os
import unittest

from collections import OrderedDict

from ihutilities.ETL_framework import (do_etl, report_input_length,
                                       check_if_already_done,
                                       get_source_generator)

from ihutilities import read_db


class TestETLFramework(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.DB_FIELDS = OrderedDict([
        ("ID"                               , "INTEGER PRIMARY KEY"),
        ("Letter"                           , "FLOAT"),
        ("Number"                           , "FLOAT"),
        ])

        cls.data_field_lookup = OrderedDict([
        ("ID"                               , "ID"),
        ("Letter"                           , "Letter"),
        ("Number"                           , "Number"),
        ])

        cls.test_root = os.path.dirname(__file__)
        cls.datapath = os.path.join(cls.test_root, "fixtures", "survey_csv.csv")
        cls.datapath2 = os.path.join(cls.test_root, "fixtures", "survey_csv2.csv")
        cls.db_config = os.path.join(cls.test_root, "fixtures", "do_etl.sqlite")

        if os.path.isfile(cls.db_config):
            os.remove(cls.db_config)

    def test_do_etl_1(self):
        _, status = do_etl(self.DB_FIELDS, self.db_config, self.datapath, self.data_field_lookup, mode="production", force=True)
        assert status == "Completed"
    
    def test_do_etl_two_stage(self):
        _, status = do_etl(self.DB_FIELDS, self.db_config, self.datapath, self.data_field_lookup, mode="production", force=True)
        _, status = do_etl(self.DB_FIELDS, self.db_config, self.datapath2, self.data_field_lookup, mode="production", force=False)
        # Check for stages one and two in the metadata table
        sql_query = "select SequenceNumber from metadata"
        results = list(read_db(sql_query, self.db_config))
        sequence_numbers = {x["SequenceNumber"] for x in results}

        assert sequence_numbers == set([1, 2])
    
    def test_do_etl_session_log(self):
        _, status = do_etl(self.DB_FIELDS, self.db_config, self.datapath, self.data_field_lookup, mode="test", chunk_size=10, force=True, chaos_monkey=True)
        mod_config, status = do_etl(self.DB_FIELDS, self.db_config, self.datapath, self.data_field_lookup, mode="test", chunk_size=10, force=False, chaos_monkey=False)
        # check for sessions 1 and 2 in the session log, check we have 35 lines in the data table
        # Check for stages one and two in the metadata table
        sql_query = "select ID from session_log"
        results = list(read_db(sql_query, mod_config))
        session_ids = {x["ID"] for x in results}

        assert session_ids == set([1, 2])

    def test_do_etl_check_malformed_rows_dropped(self):
        datapath = os.path.join(self.test_root, "fixtures", "malformed.csv")
        db_config = os.path.join(self.test_root, "fixtures", "malformed.sqlite")
        if os.path.isfile(db_config):
            os.remove(db_config)

        data_field_lookup = OrderedDict([
        ("ID"                               , 0),
        ("Letter"                           , 1),
        ("Number"                           , 2),
        ])

        db_config, status = do_etl(self.DB_FIELDS, self.db_config, datapath, data_field_lookup, mode="production", force=True, headers=False)

        sql_query = "select * from property_data;"
        results = list(read_db(sql_query, db_config))
        self.assertEqual(len(results), 3)


    def test_check_if_already_done(self):
        db_config, status = do_etl(self.DB_FIELDS, self.db_config, self.datapath, self.data_field_lookup, mode="production", force=True)
        result = check_if_already_done(self.datapath, self.db_config, "d607339fd4d5ecc01e26b18d86983f305533d20c")
        self.assertEqual(result, True)

    def test_report_input_length(self):
        test_line_limit = 1000
        file_length = report_input_length(get_source_generator, test_line_limit, self.datapath, False, ",", "utf-8-sig")
        self.assertEqual(file_length, 35)

    def test_etl_from_zip(self):
        test_root = os.path.dirname(__file__)
        datapath = os.path.join(test_root, "fixtures", "survey_csv.zip")
        db_config = os.path.join(test_root, "fixtures", "do_etl_from_zip.sqlite")

        if os.path.isfile(db_config):
            os.remove(db_config)

        db_config, status = do_etl(self.DB_FIELDS, db_config, datapath, self.data_field_lookup, mode="production", force=True)

        assert status == "Completed"
    
    def test_that_multilines_per_input_row_are_uploaded(self):
        _, status = do_etl(self.DB_FIELDS, self.db_config, self.datapath, self.data_field_lookup, rowmaker=make_row, mode="test", chunk_size=10, force=True, chaos_monkey=True)
        mod_config, status = do_etl(self.DB_FIELDS, self.db_config, self.datapath, self.data_field_lookup, rowmaker=make_row, mode="test", chunk_size=10, force=False, chaos_monkey=False)
        # check for sessions 1 and 2 in the session log, check we have 35 lines in the data table
        # Check for stages one and two in the metadata table
        sql_query = "select count(*) as cnt from property_data"
        results = list(read_db(sql_query, mod_config))
        

        assert results[0]["cnt"] == 70
    
def make_row(input_row, data_path, data_field_lookup, db_fields, null_equivalents, autoinc, primary_key):
    new_rows = []
    new_row = OrderedDict([(x, None) for x in db_fields.keys()])
     # zip input row into output row
    for output_key in new_row.keys():
        # This inserts blank fields
        value = None
        if data_field_lookup[output_key] is not None:
            value = input_row[data_field_lookup[output_key]]
        new_row[output_key] = value
                
    new_rows = [new_row]
    second_row = new_row.copy()
    second_row["Letter"] = "B"
    second_row["ID"] = str(35 + int(new_row["ID"]))
    new_rows.append(second_row)

    return new_rows

