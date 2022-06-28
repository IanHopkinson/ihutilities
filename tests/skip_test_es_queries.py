#!/usr/bin/env python
# encoding: utf-8

import unittest
import logging
import os
import socket
import time

sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
result = sock.connect_ex(("localhost", 9200))
if result == 0:
    elastic_search_not_running = False
    import elasticsearch
    from ihutilities.es_utils import es_config_template, delete_es, check_es_database_exists
else:
    elastic_search_not_running = True

from ihutilities import configure_db, write_to_db, read_db, update_to_db

# write_to_es,
#   _make_connection, read_es,
#   update_to_es, finalise_es,
#   check_mysql_database_exists)

from collections import OrderedDict

logging.basicConfig(level=logging.INFO)


# @unittest.skip("Elasticsearch is not running so skipping tests")
# @unittest.expectedFailure
@unittest.skipIf(elastic_search_not_running, "Elasticsearch is not running so skipping tests")
class ElasticsearchQueryTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        # cls.es_fields = OrderedDict([
        #       ("UPRN","INTEGER PRIMARY KEY"),
        #       ("PropertyID", "INT"),
        #       ("Addr1", "TEXT"),
        # ])

        cls.es_fields = {
            "settings": {"number_of_shards": 1, "mapper": {"dynamic": "strict"}},
            "mappings": {
                "testrecord": {
                    "properties": {
                        "UPRN": {"type": "integer"},
                        "PropertyID": {"type": "integer"},
                        "Addr1": {"type": "string"},
                        "geocode": {"type": "geo_point"},
                    }
                }
            },
        }

        test_root = os.path.dirname(__file__)
        cls.db_dir = os.path.join(test_root, "fixtures")
        if not elastic_search_not_running:
            cls.es = elasticsearch.Elasticsearch()

    def test_geopoint_query(self):
        es_config = es_config_template.copy()
        es_config["db_name"] = "test"

        # Delete "test" index if it exists
        status = delete_es(es_config)

        status = configure_db(es_config, self.es_fields, tables="testrecord", force=True)

        data = [
            {
                "UPRN": 1,
                "PropertyID": 4,
                "Addr1": "Aardvark",
                "geocode": {"lat": 63.20710, "lon": -1.89310},
            },
            {
                "UPRN": 2,
                "PropertyID": 5,
                "Addr1": "Barbarosa",
                "geocode": {"lat": 53.30710, "lon": -2.89310},
            },
            {
                "UPRN": 3,
                "PropertyID": 6,
                "Addr1": "Camel",
                "geocode": {"lat": 63.40710, "lon": -3.89310},
            },
        ]
        write_to_db(data, es_config, self.es_fields, table="testrecord")

        # Writes to Elasticsearch are not available immediately
        time.sleep(2)

        es_query = {
            "query": {
                "bool": {
                    "must": {"match_all": {}},
                    "filter": {
                        "geo_distance": {
                            "distance": "100m",
                            "geocode": {"lat": 53.30710, "lon": -2.89310},
                        }
                    },
                }
            }
        }

        results = list(read_db(es_query, es_config))

        self.assertEqual(results[0], data[1])

    def test_sort_query(self):
        es_config = es_config_template.copy()
        es_config["db_name"] = "test"

        # Delete "test" index if it exists
        status = delete_es(es_config)

        status = configure_db(es_config, self.es_fields, tables="testrecord", force=True)

        data = [
            {
                "UPRN": 1,
                "PropertyID": 2,
                "Addr1": "Aardvark",
                "geocode": {"lat": 63.20710, "lon": -1.89310},
            },
            {
                "UPRN": 2,
                "PropertyID": 2,
                "Addr1": "Barbarosa",
                "geocode": {"lat": 53.30710, "lon": -2.89310},
            },
            {
                "UPRN": 3,
                "PropertyID": 2,
                "Addr1": "Camel",
                "geocode": {"lat": 63.40710, "lon": -3.89310},
            },
        ]
        write_to_db(data, es_config, self.es_fields, table="testrecord")

        # Writes to Elasticsearch are not available immediately
        time.sleep(2)

        es_query = {
            "sort": [
                {"UPRN": {"order": "desc"}},
            ],
            "query": {
                "bool": {"must": [{"term": {"PropertyID": 2}}, {"term": {"_type": "testrecord"}}]}
            },
        }

        results = list(read_db(es_query, es_config))

        self.assertEqual(results[0], data[2])

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
