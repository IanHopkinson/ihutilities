SHELL := /bin/bash
install:
	pip install -e .
	pip install -r requirements.txt
unit_tests:
	pytest tests/
lint:
	black . --check
	flake8 .
	pylint src/ tests/
build:
	rm -rf build/
	rm -rf dist/
	python setup.py bdist_wheel