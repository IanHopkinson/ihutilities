# ihutilities - miscellaneous useful things

# Functions

## Database utilities

* configure_db 
* write_to_db
* update_to_db 
* drop_db_tables, 
* read_db
* finalise_db,
* db_config_template

## Git utilities

* git_uncommitted_changes
* git_sha

## I/O utilities

* write_dictionary
* pretty_print_dict
* sort_dict_by_value

# Issues
 
* report_input_length in ETL_framework can be really slow for a large file

# TODO

* Use setup and teardown on a per test basis for test_db_utils

* NROSH snapshot ETL using do_ETL shows that line count is misleading where we are dropping rows

* At the moment the _id in elasticsearch is autoincrementing, we should be able to explicitly set it (think there is a setting for this)

* read_es could do with returning search metadata
* Build_cache should allow you to easily calculate total run time
* Support for "resume" in ETL framework
* read_db should take templated queries (using the appropriate placeholder and data supplied separately)
* read_db should only allow select statements
* Extend testing to io_utils
* Add in some ngram functionality
* Currently using scripts for survey_csv, could use console scripts: http://python-packaging.readthedocs.org/en/latest/command-line-scripts.html