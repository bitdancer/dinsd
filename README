dinsd: D Is Not Static D
========================

This project (still in its early stages) is an attempt to turn Python into
something D-like, where D is a theoretical construct envisioned by Hugh Darwen
and C. J. Date (see http://www.thethirdmanifesto.com/).  The goal is to have a
computationally complete language that implements a full relational algebra as
a first class citizen.

Python can never be a D, because D requires static typing and Python is
dynamically typed.  But Python plus the dinsd module does make the relational
algebra a first class citizen, turning Python into Relational Python.

At this early stage of the project the persistence layer is just a backing
store for what is otherwise an in-memory database.  That is, all data for
all tables is held in memory at all times, and written to disk only when
something is updated.

Right now it is mostly useful as a tool for playing with relational algebra at
the Python shell prompt.  The goal, though, is to provide a smarter persistence
layer (as well as other currently-missing DBMS features) such that it will be
practical to use it in at least the contexts where an SQL engine like SQLite is
currently used.  Indeed, SQLite (which is used for the current persistence
layer) will be the first smarter persistence layer, if everything goes well.

Currently there is no project infrastructure other than for running the tests
('make test' in the root directory).  You can copy the package directory
(src/dinsd) to wherever you want it in order to play with it.  The only
external requirement is sqlite3.

The documentation consists of literate test documents:

    doc/relational_python.rst
    doc/db_api.rst

which are evolving along with the code and so may not be completely internally
consistent at any given checkin.  The tests should always be passing, but the
text may not be 100% consistent with itself if I'm in the middle of a major
refactoring.  Most of the time it should be, though, and the documents should
therefore provide a thorough introduction to the project and its current
capabilities.

dinsd currently requires Python 3.4 (as of this writing that means Python built
from the head of the Python "default" branch in Mercurial).  Mostly it will run
fine using Python 3.3, and I will officially support whatever the most recent
Python release is at the point where other people start wanting to play with
it.

Note that there is another project with a similar goal:

    http://www.quicksort.co.uk/DeeDoc.html

Dee is a Python 2.7 project, and does not appear to be in current development.
