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
publish:
	rm -rf build/
	rm -rf dist/
	pip install --upgrade pip
	pip install --upgrade setuptools
	pip install wheel
	pip install twine
	pip show setuptools
	python setup.py sdist bdist_wheel
	cat $(PYPIRC_PATH)
	twine upload -r project_scoped --config-file $(PYPIRC_PATH) dist/* --verbose