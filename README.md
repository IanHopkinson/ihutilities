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

* Don't create a sqlite file if read_db doesn't find a database to read

* Merge read_es functions into db_utils
* read_es could do with returning search metadata
* Build_cache should allow you to easily calculate total run time
* Support for "resume" in ETL framework
* read_db should take templated queries (using the appropriate placeholder and data supplied separately)
* read_db should only allow select statements
* Extend testing to io_utils
* Get ETL_framework to accept a function as a data source
* Add in some ngram functionality
* Currently using scripts for survey_csv, could use console scripts: http://python-packaging.readthedocs.org/en/latest/command-line-scripts.html