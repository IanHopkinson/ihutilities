#!/usr/bin/env python
# encoding: utf-8

import unittest
import logging
import os
import time
import elasticsearch

from ihutilities.es_utils import (es_config_template, delete_es,
                                  check_es_database_exists)

from ihutilities import configure_db, write_to_db, read_db, update_to_db

                                # write_to_es,
                                #   _make_connection, read_es,
                                #   update_to_es, finalise_es,
                                #   check_mysql_database_exists)

from collections import OrderedDict

logging.basicConfig(level=logging.INFO)

class ElasticsearchUtilitiesTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        # cls.es_fields = OrderedDict([
        #       ("UPRN","INTEGER PRIMARY KEY"),
        #       ("PropertyID", "INT"),
        #       ("Addr1", "TEXT"),                   
        # ])

        cls.es_fields = {
                "settings": {
                "number_of_shards": 1,
                "mapper": {
                "dynamic": "strict"
                }
                },
                "mappings": {
                    "testrecord": { 
                        "properties": {
                            "UPRN": {"type": "integer"},
                            "PropertyID": {"type": "integer"},
                            "Addr1": {"type": "string"},
                            "geocode": {"type": "geo_point"},
                        }
                    }
                 }
            }

        test_root = os.path.dirname(__file__)
        cls.db_dir = os.path.join(test_root, "fixtures")
        cls.es = elasticsearch.Elasticsearch()

    def test_configure_es(self):
        # Connect to engine and delete test table if it exists
        es_config = es_config_template.copy()
        es_config["db_name"] = "test"
        
        # Delete "test" index if it exists
        status = delete_es(es_config)

        configure_db(es_config, self.es_fields, tables="testrecord", force=True)

        # Test es exists
        exists = check_es_database_exists(es_config)
        self.assertEqual(exists, True)

        # Check details of config
        # settings = self.es.indices.get_settings(index='test')

        mappings = self.es.indices.get_mapping(index='test')

        fields = set(list(mappings["test"]["mappings"]["testrecord"]["properties"].keys()))

        self.assertEqual(set(['UPRN', 'Addr1', 'PropertyID', 'geocode']), fields)

    def test_configure_multi_doc_es(self):
        # Connect to engine and delete test table if it exists
        es_config = es_config_template.copy()
        es_config["db_name"] = "test"
        
        # Delete "test" index if it exists
        status = delete_es(es_config)

        multi_doc_fields = {}
        multi_doc_fields["testrecord"] = self.es_fields
        multi_doc_fields["testmeta"] = {
                "mappings": {
                    "testmeta": { 
                        "properties": {
                            "ID": {"type": "integer"},
                            "SecondID": {"type": "integer"},
                            "Addr2": {"type": "string"},
                            "geocode2": {"type": "geo_point"},
                        }
                    }
                 }
            }

        configure_db(es_config, multi_doc_fields, tables=["testrecord", "testmeta"], force=True)

        # Test es exists
        exists = check_es_database_exists(es_config)
        self.assertEqual(exists, True)

        # Check details of config
        # settings = self.es.indices.get_settings(index='test')

        mappings = self.es.indices.get_mapping(index='test')

        fields1 = set(list(mappings["test"]["mappings"]["testrecord"]["properties"].keys()))
        fields2 = set(list(mappings["test"]["mappings"]["testmeta"]["properties"].keys()))

        self.assertEqual(set(['UPRN', 'Addr1', 'PropertyID', 'geocode']), fields1)
        self.assertEqual(set(['ID', 'SecondID', 'Addr2', 'geocode2']), fields2)

    def test_write_to_es(self):
        # Connect to engine and delete test table if it exists
        es_config = es_config_template.copy()
        es_config["db_name"] = "test"
        
        # Delete "test" index if it exists
        status = delete_es(es_config)

        status = configure_db(es_config, self.es_fields, tables="testrecord", force=True)

        data = [{"UPRN": 1, "PropertyID": 4, "Addr1": "Aardvark", "geocode": [53.20710, -2.89310]},
                {"UPRN": 2, "PropertyID": 5, "Addr1": "Barbarosa", "geocode": [53.30710, -2.89310]},
                {"UPRN": 3, "PropertyID": 6, "Addr1": "Camel", "geocode": [53.40710, -2.89310]}]

        write_to_db(data, es_config, self.es_fields, table="testrecord")

        # Writes to Elasticsearch are not available immediately
        time.sleep(2)

        results = self.es.search(index="test", doc_type="testrecord")
        self.assertEqual(len(results["hits"]["hits"]), 3)

    def test_update_to_es(self):
       # Connect to engine and delete test table if it exists
        es_config = es_config_template.copy()
        es_config["db_name"] = "test"
        
        # Delete "test" index if it exists
        status = delete_es(es_config)

        configure_db(es_config, self.es_fields, tables="testrecord", force=True)

        data = [{"UPRN": 1, "PropertyID": 4, "Addr1": "Aardvark", "geocode": [53.20710, -2.89310]},
                {"UPRN": 2, "PropertyID": 5, "Addr1": "Barbarosa", "geocode": [53.30710, -2.89310]},
                {"UPRN": 3, "PropertyID": 6, "Addr1": "Camel", "geocode": [53.40710, -2.89310]}]

        write_to_db(data, es_config, self.es_fields, table="testrecord")

        # Writes to Elasticsearch are not available immediately
        time.sleep(2)

        update_fields = ["PropertyID", "UPRN"]
        update = [OrderedDict([("Addr1", "Some"), ("UPRN", 3)])] 
        update_to_db(update, es_config, update_fields, table="testrecord", key="UPRN")
        time.sleep(2)

        check_updated_query = {   "query": {
                                    "constant_score": {
                                      "filter": {
                                        "term": {
                                          "UPRN": 3
                                        }
                                      }
                                    }
                                  }
                                }

        results = self.es.search(index="test", doc_type="testrecord", body=check_updated_query)



        self.assertEqual(results["hits"]["hits"][0]["_source"]["Addr1"], "Some")

    def test_read_es(self):
        es_config = es_config_template.copy()
        es_config["db_name"] = "test"
        
        # Delete "test" index if it exists
        status = delete_es(es_config)

        status = configure_db(es_config, self.es_fields, tables="testrecord", force=True)

        data = [{"UPRN": 1, "PropertyID": 4, "Addr1": "Aardvark", "geocode": {"lat": 53.20710, "lon": -2.89310}},
                {"UPRN": 2, "PropertyID": 5, "Addr1": "Barbarosa", "geocode": {"lat":53.30710, "lon": -2.89310}},
                {"UPRN": 3, "PropertyID": 6, "Addr1": "Camel", "geocode": {"lat":53.40710, "lon": -2.89310}}]
        write_to_db(data, es_config, self.es_fields, table="testrecord")

        # Writes to Elasticsearch are not available immediately
        time.sleep(2)

        es_query = {"sort": { "UPRN" : {"order" : "asc"}}}
        
        results = list(read_db(es_query, es_config))

        for i, result in enumerate(results):
            self.assertEqual(result, data[i])

    def test_geopoint_query(self):
        es_config = es_config_template.copy()
        es_config["db_name"] = "test"
        
        # Delete "test" index if it exists
        status = delete_es(es_config)

        status = configure_db(es_config, self.es_fields, tables="testrecord", force=True)

        data = [{"UPRN": 1, "PropertyID": 4, "Addr1": "Aardvark", "geocode": {"lat": 63.20710, "lon": -1.89310}},
                {"UPRN": 2, "PropertyID": 5, "Addr1": "Barbarosa", "geocode": {"lat":53.30710, "lon": -2.89310}},
                {"UPRN": 3, "PropertyID": 6, "Addr1": "Camel", "geocode": {"lat":63.40710, "lon": -3.89310}}]
        write_to_db(data, es_config, self.es_fields, table="testrecord")

        # Writes to Elasticsearch are not available immediately
        time.sleep(2)

        es_query = {"query": {
                        "bool" : {
                            "must" : {
                                "match_all" : {}
                            },
                            "filter" : {
                                "geo_distance" : {
                                    "distance" : "100m",
                                    "geocode" : {
                                        "lat" : 53.30710,
                                        "lon" : -2.89310
                                    }
                                }
                            }
                        }
                        }
                    }
        
        results = list(read_db(es_query, es_config))

        self.assertEqual(results[0], data[1])

    # def test_geopoint_query(self):
    #     es_config = es_config_template.copy()
    #     es_config["db_name"] = "test"
        
    #     # Delete "test" index if it exists
    #     status = delete_es(es_config)

    #     status = configure_es(es_config, self.es_fields, tables="testrecord", force=True)

    #     data = [{"UPRN": 1, "PropertyID": 4, "Addr1": "Aardvark", "geocode": {"lat": 53.20710, "lon": -2.89310}},
    #             {"UPRN": 2, "PropertyID": 5, "Addr1": "Barbarosa", "geocode": {"lat":53.30710, "lon": -2.89310}},
    #             {"UPRN": 3, "PropertyID": 6, "Addr1": "Camel", "geocode": {"lat":53.40710, "lon": -2.89310}}]

    #     write_to_es(data, es_config, self.es_fields, table="testrecord")

    #     # Writes to Elasticsearch are not available immediately
    #     time.sleep(2)

    #     es_query = {"query" : {"match_all" : {}}}
        
    #     results = list(read_es(es_query, es_config))

    #     for i, result in enumerate(results):
    #         self.assertEqual(result, data[i])

    # def test_check_database_exists(self):
    #     db_config = db_config_template.copy()
    #     db_config["db_name"] = "djnfsjnf"

    #     assert_equal(check_mysql_database_exists(db_config), False)

    #     db_config["db_name"] = "INFORMATION_SCHEMA"
    #     assert_equal(check_mysql_database_exists(db_config), True)
