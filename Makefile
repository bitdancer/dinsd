PYTHON=/home/rdmurray/python/p33/python
PYTHONPATH := src

test:
	$(PYTHON) -m doctest doc/relational_python.rst
	-DINSD_DB_MODULE_TO_TEST=dinsd.sqlite_pickle_db \
	 	DINSD_TEST_DB_URI='/tmp/dinsd_test.db' \
	 	$(PYTHON) -m doctest doc/db_api.rst
	rm /tmp/dinsd_test.db
