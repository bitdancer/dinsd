PYTHON=/usr/bin/python3
PYTHONPATH := src

test: test_relational_python test_sqlite_pickle_db

test_relational_python:
	$(PYTHON) -m doctest doc/relational_python.rst

test_sqlite_pickle_db:
	-DINSD_DB_MODULE_TO_TEST=dinsd.sqlite_pickle_db \
	 	DINSD_TEST_DB_URI='/tmp/dinsd_test.db' \
	 	$(PYTHON) -m doctest doc/db_api.rst
	rm /tmp/dinsd_test.db
