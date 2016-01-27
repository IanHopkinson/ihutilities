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

* Attempting to write in chunks of more than 1000 lines produces a "force closed connection" error on MariaDB 
* report_input_length in ETL_framework can be really slow for a large file

# TODO

* Document functions with a docstring (prepare for Sphinx, even if we don't use it now https://pythonhosted.org/an_example_pypi_project/sphinx.html). Start with configure_db 
* read_db should take templated queries (using the appropriate placeholder and data supplied separately)
* read_db should only allow select statements
* Extend testing to io_utils
* Get ETL_framework to accept a function as a data source
* Add in some ngram functionality
* make a survey_csv.py function
* Currently using scripts for survey_csv, could use console scripts: http://python-packaging.readthedocs.org/en/latest/command-line-scripts.html