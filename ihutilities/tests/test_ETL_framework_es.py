#!/usr/bin/env python
# encoding, utf-8

import os
import unittest
import time

from collections import OrderedDict

from ihutilities.ETL_framework import (do_etl, check_if_already_done)

from ihutilities.es_utils import configure_es, write_to_es, es_config_template, delete_es


class TestETLFramework_es(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.DB_FIELDS = {
            "property_data": {
                "properties": {
                    "ID": {"type": "integer"},
                    "Letter": {"type": "string"},
                    "Number": {"type": "string"}
                }
            }
            }

        cls.data_field_lookup = OrderedDict([
        ("ID"                               , "ID"),
        ("Letter"                           , "Letter"),
        ("Number"                           , "Number"),
        ])

        cls.test_root = os.path.dirname(__file__)
        cls.datapath = os.path.join(cls.test_root, "fixtures", "survey_csv.csv")
        cls.datapath2 = os.path.join(cls.test_root, "fixtures", "survey_csv2.csv")
        cls.db_config = es_config_template.copy()
        cls.db_config["db_name"] = "etlfixture"

        delete_es(cls.db_config)

    def test_do_etl_1(self):
        _, status = do_etl(self.DB_FIELDS, self.db_config, self.datapath, self.data_field_lookup, mode="production", force=True)
        assert status == "Completed"
    
    def test_do_etl_two_stage(self):
        _, status = do_etl(self.DB_FIELDS, self.db_config, self.datapath, self.data_field_lookup, mode="production", force=True)
        _, status = do_etl(self.DB_FIELDS, self.db_config, self.datapath2, self.data_field_lookup, mode="production", force=False)
        assert status == "Completed"
        # Check for stages one and two in the metadata table
        # sql_query = "select SequenceNumber from metadata"
        # results = list(read_db(sql_query, self.db_config))
        # sequence_numbers = {x["SequenceNumber"] for x in results}

        # assert sequence_numbers == set([1, 2])
    
    def test_do_etl_session_log(self):
        _, status = do_etl(self.DB_FIELDS, self.db_config, self.datapath, self.data_field_lookup, mode="test", chunk_size=10, force=True, chaos_monkey=True)
        
        mod_config, status = do_etl(self.DB_FIELDS, self.db_config, self.datapath, self.data_field_lookup, mode="test", chunk_size=10, force=False, chaos_monkey=False)
        
        assert status == "Completed"
        # check for sessions 1 and 2 in the session log, check we have 35 lines in the data table
        # Check for stages one and two in the metadata table
        # sql_query = "select ID from session_log"
        # results = list(read_db(sql_query, mod_config))
        # session_ids = {x["ID"] for x in results}

        # assert session_ids == set([1, 2])

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
        assert status == "Completed"
        # sql_query = "select * from property_data;"
        # results = list(read_db(sql_query, db_config))
        # self.assertEqual(len(results), 3)


    def test_check_if_already_done(self):
        mod_config, status = do_etl(self.DB_FIELDS, self.db_config, self.datapath, self.data_field_lookup, mode="production", force=True)
        time.sleep(2)
        result = check_if_already_done(self.datapath, mod_config, "d607339fd4d5ecc01e26b18d86983f305533d20c")
        self.assertEqual(result, True)

