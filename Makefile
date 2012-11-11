PYTHON=/home/rdmurray/python/p33/python
PYTHONPATH := src

test:
	$(PYTHON) -m unittest test
	ls src/test/*.txt | xargs $(PYTHON) -m doctest
