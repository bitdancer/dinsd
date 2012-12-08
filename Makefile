PYTHON=/home/rdmurray/python/p33/python
PYTHONPATH := src

test:
	ls doc/*.rst | xargs $(PYTHON) -m doctest
