#!/usr/bin/env python
# encoding: utf-8
"""
This module contains utility functions relating to databases, git and I/O
"""
from .db_utils import (configure_db, write_to_db, update_to_db, 
                       drop_db_tables, read_db, finalise_db,
                       db_config_template, check_mysql_database_exists)
from .git_utils import git_uncommitted_changes, git_sha, git_calculate_file_sha
from .io_utils import write_dictionary, pretty_print_dict, sort_dict_by_value, calculate_file_sha
from .shapefile_utils import (load_shapefile_data, summarise_shapefile, plot_shapefile,
                              make_bbox_polygon, make_linestring, make_multipolygon,
                              make_polygon)
# from .ETL_framework import do_ETL