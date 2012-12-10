Databases
=========

Copyright 2012 by R. David Murray, Licensed under the Apache License, Version
2.0 (http://www.apache.org/licenses/LICENSE-2.0).


Introduction
------------

This document covers the persistent database API which all dinsd persistent
database modules are expected to provide.  Because this document is an API
validation document, some of the tests will be a bit more complex that is
optimal for documentation.  As with the ``Relational Python`` document, this
document is a mixture of explanation and tests, as well as being the
development document for the API.

In this document I am still following along with AIRDT (see ``Relational
Python`` for more information on that reference), beginning with Chapter 6.

As implied by the first paragraph above, dinsd has more than one persistent
database module.  Which one you use depends on your specific needs.  As of this
writing there is in fact only one, the ``sqlite_pickle_db`` module.  There is
likely to be a lot of revision happening in this file when there starts to be
more than one.

In order to use this as an API validation document, we need to make the import
of the names we will be testing conditional on which database module we are
testing.  We do this via an environment variable set by the testing
infrastructure:

    >>> import os
    >>> test_mod_name = os.getenv('DINSD_DB_MODULE_TO_TEST',
    ...                           'dinsd.sqlite_pickle_db')
    >>> import importlib
    >>> test_mod = importlib.import_module(test_mod_name)
    >>> Database = getattr(test_mod, 'Database')

As you can see above, the main name that we are concerned with testing and
describing here is the ``Database`` class provided by the persistent database
module under test.



Defining and Accessing a Database
---------------------------------

A ``Database`` object is functionally a connection to a persistent backing
store.

To create or access a database, you create a ``Database`` connection object
and tell it how to connect to the database by passing it a database URI.
Since the URI is database-specific, we'll need to fetch that from the
test environment as well:

    >>> dburi = os.getenv('DINSD_TEST_DB_URI')

Creating the database connection is simple:

    >>> db = Database(dburi)

If the test infrastructure is working correctly, this database will initially
be empty:

    >>> db
    Database({})
    
We need some relations to work with, which means replicating the data
definitions from ``Relational Python``, since we are still following AIRDT.
To make this easier the ``SID`` and ``CID`` classes are defined in
our ``test_support`` test module.

    >>> from dinsd import rel, row, expression_namespace
    >>> from test_support import SID, CID
    >>> expression_namespace['CID'] = CID
    >>> expression_namespace['SID'] = SID
    >>> IsCalled = rel(name=str, student_id=SID)
    >>> is_called = IsCalled(
    ...     ('student_id',  'name'),
    ...     ('S1',          'Anne'),
    ...     ('S2',          'Boris'),
    ...     ('S3',          'Cindy'),
    ...     ('S4',          'Devinder'),
    ...     ('S5',          'Boris'),
    ...     )
    >>> IsEnrolledOn = rel(course_id=CID, student_id=SID)
    >>> is_enrolled_on = IsEnrolledOn(
    ...     ('student_id',  'course_id'),
    ...     ('S1',          'C1'),
    ...     ('S1',          'C2'),
    ...     ('S2',          'C1'),
    ...     ('S3',          'C3'),
    ...     ('S4',          'C1'),
    ...     ('S2',          'C3'),
    ...     )
    >>> ExamMarks = rel(course_id=CID, student_id=SID, mark=int)
    >>> exam_marks = rel(student_id=SID, course_id=CID, mark=int)(
    ...     ('student_id', 'course_id', 'mark'),
    ...     ('S1',         'C1',        85),
    ...     ('S1',         'C2',        49),
    ...     ('S1',         'C3',        85),
    ...     ('S2',         'C1',        49),
    ...     ('S3',         'C3',        66),
    ...     ('S4',         'C1',        93),
    ...     )

The database object acts like a Python dictionary: the relations that are
stored in the database are entries in that dictionary.  This avoids any naming
conflicts with the methods defined on the ``Database`` object.  We do this
rather than use attribute notation and the ``_xxx_`` naming convention because
it is much more common to access the methods of the database object than it is
to access the non-relational-attribute attributes of a ``row``.  It is also
helpful to think of the dictionary as the "``TUPLE`` of relations" discussed in
AIRDT, and the rest of the attributes of the ``Database`` objects as the
controls for the database management system.

We persist a relation into the database by storing it in the ``Database``:

    >>> db['is_called'] = is_called
    >>> db                                  # doctest: +NORMALIZE_WHITESPACE
    Database({'is_called': <class 'dinsd.PersistentRelation({'name': str,
         'student_id': SID})'>})

The ``repr`` of a ``Database`` indicates that it is a set of names mapped to
relation types.  Using AIRDT's terminology, it is a ``TUPLE`` of relations, but
in the ``repr`` we show only the types, not the content.  Note that unlike
other dinsd objects, this repr cannot be evaluated to obtain an equivalent
object (if you were to eval it, you'd end up with a database with relations of
equivalent *type*, but all empty).

Using attribute syntax to access the persistent relations is often much more
convenient than using dictionary syntax, so dinsd databases also support it,
though a special attribute ``r``:

    >>> print(db.r.is_called)
    +----------+------------+
    | name     | student_id |
    +----------+------------+
    | Anne     | S1         |
    | Boris    | S2         |
    | Boris    | S5         |
    | Cindy    | S3         |
    | Devinder | S4         |
    +----------+------------+

We can also create a persistent relation by supplying just the type:

    >>> db.r.is_enrolled_on = IsEnrolledOn
    >>> db                                  # doctest: +NORMALIZE_WHITESPACE
    Database({'is_called': <class 'dinsd.PersistentRelation({'name': str,
         'student_id': SID})'>, 'is_enrolled_on': <class
         'dinsd.PersistentRelation({'course_id': CID, 'student_id': SID})'>})

At this point, ``db.r.is_called`` has content, but ``is_enrolled_on`` is an
empty relation:

    >>> len(db.r.is_called)
    5
    >>> len(db.r.is_enrolled_on)
    0

We can provide content for ``is_enrolled_on`` by assigning our relation that
has content to the attribute:

    >>> db.r.is_enrolled_on = is_enrolled_on
    >>> len(db.r.is_enrolled_on)
    6

It is an error to try to assign a relation of the wrong type to a relation
attribute:

    >>> db.r.is_enrolled_on = is_called       # doctest: +NORMALIZE_WHITESPACE
    Traceback (most recent call last):
        ...
    ValueError: header mismatch: a value of type <class 'dinsd.rel({'name': str,
        'student_id': SID})'> cannot be assigned to a database relation of type
         <class 'dinsd.PersistentRelation({'course_id': CID,
         'student_id': SID})'>

Indeed, it is an error to try to anything that is not of the correct type:

    >>> db.r.is_enrolled_on = 1
    Traceback (most recent call last):
        ...
    ValueError: Only relations may be stored in database, not <class 'int'>

However, wholesale assignment is not the typical way to update a relation in a
database.  We'll talk about the alternatives later.

We prove that the backing store works by closing the database, reopening it,
and verifying that the data is still be there:

    >>> db.close()
    >>> db.r.is_called
    Traceback (most recent call last):
        ...
    KeyError: 'is_called'
    >>> del db

    >>> db = Database(dburi)
    >>> print(db.r.is_called)
    +----------+------------+
    | name     | student_id |
    +----------+------------+
    | Anne     | S1         |
    | Boris    | S2         |
    | Boris    | S5         |
    | Cindy    | S3         |
    | Devinder | S4         |
    +----------+------------+

We can create a relation via the ``r`` attribute as well:

    >>> db.r.exam_marks = exam_marks

A very important note: unlike a normal dictionary, the relation sorted in the
``Database`` is *not* the same object that we assigned to it:

    >>> db.r.exam_marks is exam_marks
    False

As we saw in the ``Database`` repr above, it isn't even the same Python type:

    >>> type(db.r.exam_marks) == type(exam_marks)
    False

The *headers*, however, are the same:

    >>> db.r.exam_marks.header == exam_marks.header
    True

Which means they are of the same *relational* type.

Since relations are treated as read-only objects, much of the time this
distinction does not matter.  But occasionally it does (we'll see an example
below), so it is best to be aware of it.

Because it sometimes matters, and because this document is testing the database
and not the base algebra, we'll switch our names to be pointing to the database
relations.  But we'll keep pointers to the non-database versions, to use to
demonstrate the places where it matters which you use.

    >>> is_called_save = is_called
    >>> is_enrolled_on_save = is_enrolled_on
    >>> exam_marks_save = exam_marks
    >>> is_called = db.r.is_called
    >>> is_enrolled_on = db.r.is_enrolled_on
    >>> exam_marks = db.r.exam_marks



Constraints
-----------

It seems to me that defining anything other than value-level constraints on a
computed relation doesn't make much sense.  Although AIRDT doesn't address
this question directly, all of his examples that have constraints above the
value level are relations defined in the database (``relvars`` in AIRDT
parlance).

So I've postponed any discussion of constraints until now, when we've
introduced the mechanism for storing a relation in a database.

I'm not going to go through any of the theoretical discussions or examples
from AIRDT on the general topic of constraints.  I'm only going to talk about,
and give examples of, defining constraints of various types.

Unlike *Tutorial D*, we are hoping that this API will be useful in production
code (though probably not this implementation of it), so unlike *Tutorial D*
we do provide specific ways to define constraints at each of the four levels
of interest: value level, row level, relation level, and database level.  As
explained in Chapter 6 of AIRDT, all of these *can* be implemented as database
level constraints.  But it is more efficient, and easier to do, if we define
them at the appropriate level using level-specific mechanisms.


Value Level Constraints
~~~~~~~~~~~~~~~~~~~~~~~

Value level constraints are most efficiently defined by defining a custom
type.  We did that with ``SID`` and ``CID``.  However, it can sometimes
be more convenient to define them using row level constraints.  We
give an example of doing that in the next section.


Row Level Constraints
~~~~~~~~~~~~~~~~~~~~~

Following AIRDT, our example of using the row level constraint mechanism is
actually a value level constraint.  We will constrain the integer values of
the ``mark`` attribute in ``exam_marks`` to be between ``0`` and ``100``,
inclusive:

    >>> db.constrain_rows('exam_marks', valid_mark="0 <= mark <= 100")

Note that we pass the *name* of the database relation attribute, not a
relation.  This is because the constraints are being set on the named
attribute, not on a relation object.  The keyword assigns a name to a
constraint expression; we'll see later how that can be used.  The constraint
expression works just like the row expressions we've already seen.  It's value
is treated as a boolean, and if that boolean value is ``True``, the row
satisfies the constraint and all is well.  If that value is ``False``, the row
does not satisfy the constraint, and is therefore not a valid row for the
specified database relation.

With this constraint in place, we can no longer assign a relation that
contains values outside of that range to the database's ``exam_marks``
relation attribute:

    >>> db.r.exam_marks = ~row(student_id=SID('S1'),
    ...                        course_id=CID('C1'),
    ...                        mark=102)
    ...
    ... # doctest: +NORMALIZE_WHITESPACE
    Traceback (most recent call last):
        ...
    dinsd.db.RowConstraintError: exam_marks constraint valid_mark violated:
         '0 <= mark <= 100' is not satisfied by row({'course_id': CID('C1'),
         'mark': 102, 'student_id': SID('S1')})

When a constraint violation happens, the database relation is not updated:

    >>> print(db.r.exam_marks)
    +-----------+------+------------+
    | course_id | mark | student_id |
    +-----------+------+------------+
    | C1        | 49   | S2         |
    | C1        | 85   | S1         |
    | C1        | 93   | S4         |
    | C2        | 49   | S1         |
    | C3        | 66   | S3         |
    | C3        | 85   | S1         |
    +-----------+------+------------+

Conversely, if we attempt to define a constraint that the existing database
relation does not satisfy, we will also get a constraint violation:


    >>> db.constrain_rows('exam_marks', valid_mark="50 <= mark <= 100")
    ...
    ... # doctest: +NORMALIZE_WHITESPACE
    Traceback (most recent call last):
        ...
    dinsd.db.RowConstraintError: exam_marks constraint valid_mark violated:
        '50 <= mark <= 100' is not satisfied by row({'course_id': CID('C1'),
         'mark': 49, 'student_id': SID('S2')})

In this case, it is the list of constraints that is not updated.  The database
relation is again unchanged, and the database still conforms to all of the
active constraints:

    >>> db.row_constraints['exam_marks']
    {'valid_mark': '0 <= mark <= 100'}

We can define more than one constraint for a database relation, and we
can define more than one in a single call:

    >>> db.constrain_rows('exam_marks', valid_sid="student_id!=SID('S0')",
    ...                                   valid_cid="course_id!=CID('C0')")

    >>> sorted(db.row_constraints['exam_marks'].items())
    ...
    ... # doctest: +NORMALIZE_WHITESPACE
    [('valid_cid', "course_id!=CID('C0')"), ('valid_mark',
         '0 <= mark <= 100'), ('valid_sid', "student_id!=SID('S0')")]

    >>> db.r.exam_marks = ~row(student_id=SID('S1'),
    ...                        course_id=CID('C0'),
    ...                        mark=99)
    ...
    ... # doctest: +NORMALIZE_WHITESPACE
    Traceback (most recent call last):
        ...
    dinsd.db.RowConstraintError: exam_marks constraint valid_cid violated:
        "course_id!=CID('C0')" is not satisfied by row({'course_id': CID('C0'),
        'mark': 99, 'student_id': SID('S1')})

    >>> db.r.exam_marks = ~row(student_id=SID('S0'),
    ...                        course_id=CID('C1'),
    ...                        mark=99)
    ...
    ... # doctest: +NORMALIZE_WHITESPACE
    Traceback (most recent call last):
        ...
    dinsd.db.RowConstraintError: exam_marks constraint valid_sid violated:
        "student_id!=SID('S0')" is not satisfied by row({'course_id': CID('C1'),
        'mark': 99, 'student_id': SID('S0')})

Unlike other dinsd functions that take expressions, it is *not* valid to use
a function or lambda as a constraint:

    >>> db.constrain_rows('exam_marks', invalid=lambda r: r.mark < 100)
    ...
    ... # doctest: +ELLIPSIS
    Traceback (most recent call last):
        ...
      not ((<function <lambda> at 0xb6c1f394>) and (course_id!=CID('C0'))...
    SyntaxError: invalid syntax

(Note: even more than some of the others, this error may well change.)

This is because the constraints are stored in the persistent store, and it is
not necessarily practical to store Python function definitions in the
persistent store.

    >>> x = db.row_constraints.copy()
    >>> db.close()
    >>> db.row_constraints
    defaultdict(<class 'dict'>, {})
    >>> db = Database(dburi)
    >>> db.row_constraints == x
    True

You cannot define a row constraint on a relation that doesn't exist:

    >>> db.constrain_rows('foo', bar='True')
    Traceback (most recent call last):
        ...
    KeyError: 'foo'

Unlike the case with relation and row headers, it is unlikely that an
application will accidentally update the dictionary of row constraints.  dinsd
therefore, in the Python consenting adults fashion, does not try to protect
you from doing so.  If you modify the ``row_constraints`` dictionary, the
in-memory database constraints will cease to match the constraints in the
persistent store, which is likely to lead to undesirable results.  So don't do
that unless you've thought of a really good reason and are willing to
risk shooting yourself in the foot and screwing up your data.

Unlike other named dinsd objects, which exist as attributes and so are
restricted from having names that start with ``_``, constraints are never
accessed as attributes, and so there is no restriction on their names other
than that they be Python identifiers:

    >>> db.constrain_rows('is_called', _no_foos_allowed="name!='foo'")
    >>> db.r.is_called = ~row(name='foo', student_id=SID('S42'))
    ...
    ... # doctest: +NORMALIZE_WHITESPACE
    Traceback (most recent call last):
        ...
    dinsd.db.RowConstraintError: is_called constraint _no_foos_allowed violated:
        "name!='foo'" is not satisfied by row({'name': 'foo', 'student_id':
        SID('S42')})

Constraints may also be deleted:

    >>> db.remove_row_constraints('is_called', '_no_foos_allowed')
    >>> db.row_constraints['is_called']
    {}
    >>> db.close()
    >>> db = Database(dburi)
    >>> db.row_constraints['is_called']
    {}

Just as more than one constraint can be added at a time, multiple
constraints may be deleted in a single call:

    >>> db.remove_row_constraints('exam_marks', 'valid_sid', 'valid_cid')
    >>> db.row_constraints['exam_marks']
    {'valid_mark': '0 <= mark <= 100'}

Of course, you can't delete a constraint that doesn't exist:

    >>> db.remove_row_constraints('exam_marks', 'valid_sid')
    Traceback (most recent call last):
        ...
    KeyError: 'valid_sid'

Or from a relation that doesn't exist:

    >>> db.remove_row_constraints('foo', 'bar')
    Traceback (most recent call last):
        ...
    KeyError: 'foo'


Relation Level Constraints
~~~~~~~~~~~~~~~~~~~~~~~~~~

dinsd currently supports only one relation level constraint, and that is the
special constraint "key".  *Tutorial D* requires that every relation stored
in the database have a ``KEY`` declaration, which is intended to be the
minimal key for the relation (that is, the key for which there is no key
containing fewer columns that still guarantees that each row is unique).
dinsd does not make this requirement, but neither does it pretend to know the
minimal key in the absence of a key declaration.  (That is, unlike some
systems, it does not default to using the entire row as the key.)

In *Tutorial D* a key declaration is the keyword ``KEY`` and a list of columns
that follows a relation declaration.  We've seen an example of this before:

    VAR IS_CALLED BASE
    INIT (ENROLMENT { StudentId, Name })
    KEY { StudentId } ;
    VAR IS_ENROLLED_ON BASE
    INIT (ENROLMENT { StudentId, CourseId })
    KEY { StudentId, CourseId } ;

In dinsd, we use the ``set_key`` method of the ``Database`` object:

    >>> db.set_key('is_called', {'student_id'})
    >>> db.set_key('is_enrolled_on', {'student_id', 'course_id'})
    >>> db.set_key('exam_marks', {'student_id', 'course_id'})

As with row constraints, the key constraint is a property of a relation stored
in a database, and not a property of the relation itself.  So to query the
keys we ask the ``Database`` object:

    >>> sorted(db.key('is_enrolled_on'))
    ['course_id', 'student_id']

The ``display`` function indicates the keys of a database relation by
using ``=`` characters in the table header separator for key columns:

    >>> print(exam_marks.display('student_id', 'course_id', 'mark'))
    +------------+-----------+------+
    | student_id | course_id | mark |
    +============+===========+------+
    | S1         | C1        | 85   |
    | S1         | C2        | 49   |
    | S1         | C3        | 85   |
    | S2         | C1        | 49   |
    | S3         | C3        | 66   |
    | S4         | C1        | 93   |
    +------------+-----------+------+

This, by the way, is the first of those places where it matters whether the
relation is the database object or not.  The original relation (the
non-database one) doesn't have a key constraint, and so display does
not show any '='s:

    >>> print(exam_marks_save.display('student_id', 'course_id', 'mark'))
    +------------+-----------+------+
    | student_id | course_id | mark |
    +------------+-----------+------+
    | S1         | C1        | 85   |
    | S1         | C2        | 49   |
    | S1         | C3        | 85   |
    | S2         | C1        | 49   |
    | S3         | C3        | 66   |
    | S4         | C1        | 93   |
    +------------+-----------+------+

XXX: key constraints aren't fully working yet.  Nor are they saved.
