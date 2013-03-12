#Copyright 2012, 2013 R. David Murray (see end comment for terms).
import collections as _collections
import contextlib as _contextlib
import functools as _functools
import pickle as _pickle
import sqlite3 as _sqlite
import threading as _threading
import weakref as _weakref
import dinsd as _dinsd
from dinsd import (rel as _rel, expression_namespace as _all, _Relation,
                   _hsig, display as _display)
from dinsd.db import (ConstraintError, RowConstraintError, DBConstraintLoop,
                      Rollback)

# For debugging only.
import sys as _sys
def ___(*args):
    print(*args, file=_sys.stderr, flush=True)
    return args[-1]
_all['___'] = ___


class PersistentRelation(_Relation):

    def __init__(self, db, name, *args):
        self.db = db
        self.name = name
        self.key = None
        super().__init__(*args)

    # Local Decorator.
    def _transaction_required(meth):
        @_functools.wraps(meth)
        def transaction_required_wrapper(self, *args, **kw):
            if self.db.transactions:
                meth(self, *args, **kw)
            else:
                with self.db.transaction():
                    meth(self, *args, **kw)
        return transaction_required_wrapper

    def __str__(self):
        return self.display(*sorted(self.header))

    def display(self, *args, **kw):
        if 'highlight' not in kw:
            kw['highlight'] = getattr(self, 'key', [])
        return _display(self, *args, **kw)

    def copy(self):
        new = type(self)(self.db, self.name)
        new._rows = set(self._rows)
        new.key = self.key
        return new

    @_transaction_required
    def insert(self, rows):
        if hasattr(rows, '_header_'):
            rows = ~rows
        new = self.copy()
        for rw in rows:
            if rw in new._rows:
                raise ConstraintError("row {} already in relation".format(rw))
            if rw._header_ != self.header:
                raise TypeError("Type of inserted row ({}) does not match "
                                "type of relation ({})".format(rw._header_,
                                                               self.header))
            self.db._check_row_constraint(self.name, new, rw)
            self.db._insert_row(self.name, rw)
            new._rows.add(rw)
        self.db._transaction_ns.current[self.name] = new
        self.db._check_db_constraints()

    @_transaction_required
    def update(self, condition, **kw):
        if isinstance(condition, str):
            c = compile(condition, '<update>', 'eval')
            condition = lambda r, c=c: eval(c, _all, r._as_locals())
        changes = {}
        for n, f in kw.items():
            if n not in self.header:
                raise ValueError("Unknown attribute name {!r}".format(n))
            if isinstance(f, str):
                c = compile(f, '<update-'+n+'>', 'eval')
                changes[n] = lambda r, c=c: eval(c, _all, r._as_locals())
        self.db._transaction_ns.current[self.name] = new = self.copy()
        for rw in self:
            if not condition(rw):
                continue
            new._rows.remove(rw)
            # XXX This update can be made WAY more efficient.
            self.db._update_key(self.name)
            new_rw = rw.copy()
            updates = {}
            for attrname, change in changes.items():
                val = change(rw)
                setattr(new_rw, attrname, val)
                updates[attrname] = val
            self.db._check_row_constraint(self.name, new, new_rw)
            # XXX this is very inefficient, but we can't do better until we
            # learn to parse strings and turn them into SQL...so this is
            # probably the place to start building that translator.
            self.db._update_row(self.name, new_rw >> self.key.keys(), updates)
            new._rows.add(new_rw)
            self.db._update_key(self.name)
        self.db._transaction_ns.current[self.name] = new
        self.db._check_db_constraints()

    @_transaction_required
    def delete(self, condition):
        if isinstance(condition, str):
            c = compile(condition, '<delete>', 'eval')
            condition = lambda r, c=c: eval(c, _all, r._as_locals())
        new = self.copy()
        for rw in self:
            if not condition(rw):
                continue
            new._rows.remove(rw)
        self.db._transaction_ns.current[self.name] = new
        self.db._check_db_constraints()

            
class DisconnectedPersistentRelation:
    pass


# There isn't likely to be much memory savings from doing this, but we need a
# place to construct the type anyway, so it might as well be a registry.

_persistent_type_registry = _weakref.WeakValueDictionary()

def _get_persistent_type(r):
    hsig = _hsig(r.header)
    cls = _persistent_type_registry.get(hsig)
    if cls is None:
        rcls = r.__class__
        dct = dict(rcls.__dict__)
        name = PersistentRelation.__name__ + '(' + rcls.__name__.split('(', 1)[1]
        cls = type(name, (PersistentRelation,), dct)
        _persistent_type_registry[hsig] = cls
    return cls


class _R:

    """Provides attribute style access to Database relations."""

    def __init__(self, db):
        self._db = db

    def __getattr__(self, name):
        return self._db[name]

    def __setattr__(self, name, val):
        if name.startswith('_'):
            super().__setattr__(name, val)
            return
        self._db[name] = val


class _DBCon(_threading.local):

    def __init__(self, storage):
        self.storage = storage
        self.con = storage.new_con()

    def __enter__(self):
        return self.con.__enter__()

    def __exit__(self, *args, **kw):
        self.con.__exit__(*args, **kw)


class Database(dict):

    def __init__(self, fn, debug_sql=False):
        self._storage = _dumb_sqlite_persistence(fn, debug_sql=False)
        self._init()
        self.r = _R(self)
        with self._con as con:
            con.initialize_sqlite_db_if_needed()
            self.row_constraints.update(con.get_row_constraints())
            for name, r in con.relations():
                super().__setitem__(name, _get_persistent_type(r)(self, name, r))

    def _init(self):
        self.row_constraints = _collections.defaultdict(dict)
        self._system_relations = {}
        self._system_ns = _dinsd._NS(self._system_relations)
        self._constraints = {}
        self._transaction_ns = _dinsd._NS(self, in_getitem=False)
        self._con = _DBCon(self._storage)

    def _as_locals(self):
        n = [_dinsd.ns.current]
        if not self.transactions:
            n.append(self)
        return _collections.ChainMap(self._system_ns.current, *n)

    @_contextlib.contextmanager
    def transaction(self):
        changes = {}
        self._transaction_ns.push(changes)
        _dinsd.ns.push(self._transaction_ns.current)
        system_changes = {}
        self._system_ns.push(system_changes)
        try:
            yield
        except Rollback:
            return
        finally:
            _dinsd.ns.pop()
            self._transaction_ns.pop()
            self._system_ns.pop()
        if self.transactions:
            self._transaction_ns.current.maps[1].update(changes)
            self._system_ns.current.maps[1].update(system_changes)
        else:
            # These two operations also update the dicts the base chainmaps in
            # the namespaces are wrapped around.
            self._update_db_rels(changes)
            self._system_relations.update(system_changes)

    @property
    def transactions(self):
        return len(self._transaction_ns) - 1

    # Local Decorator.
    def _transaction_required(meth):
        @_functools.wraps(meth)
        def wrapper(self, *args, **kw):
            if self.transactions:
                meth(self, *args, **kw)
            else:
                with self.transaction():
                    meth(self, *args, **kw)
        return wrapper

    def _update_db_rels(self, updated_rels):
        with self._con as con:
            for name, val in updated_rels.items():
                oldval = self.get(name)
                if oldval is None:
                    con.add_reltype(name, val.header)
                if val != oldval:
                    con.update_relation(name, val)
        for name, val in updated_rels.items():
            # XXX this can be made more efficient.
            if getattr(val, 'db', None) == self:
                super().__setitem__(name, val)
            else:
                super().__setitem__(name, _get_persistent_type(val)(self,
                                                                    name,
                                                                    val))

    def __getitem__(self, name):
        # XXX I wonder if there is a more elegant way to to do this.
        if self._transaction_ns.in_getitem:
            self._transaction_ns.in_getitem = False
            return super().__getitem__(name)
        self._transaction_ns.in_getitem = True
        return self._transaction_ns.current[name]

    @_transaction_required
    def __setitem__(self, name, val):
        if not hasattr(val, 'header'):
            raise ValueError("Only relations may be stored in database, "
                "not {}".format(type(val)))
        attr = self.get(name)
        if attr is not None:
            if val.header != attr.header:
                raise ValueError("header mismatch: a value of type {} cannot "
                    "be assigned to a database relation of type {}".format(
                        type(val), type(attr)))
            if isinstance(val, type):
                raise ValueError("database relation type already set")
        elif isinstance(val, type):
            val = val()
        # XXX Do we need to use the DB relation in _check_constraints?
        self._check_constraints(name, val)
        self._transaction_ns.current[name] = val

    def _insert_row(self, relname, rw):
        with self._con as con:
            con.insert_row(relname, rw)

    def _update_row(self, relname, key, changes):
        with self._con as con:
            con.update_row(relname, key.__dict__, changes)

    def __repr__(self):
        return "{}({{{}}})".format(
            self.__class__.__name__,
            ', '.join("{!r}: {!r}".format(n, type(r))
                      for n, r in sorted(self.items())))

    # Constraint checking support.  A transaction MUST be active when the
    # constraint checks are called.

    def _check_constraints(self, relname, r):
        self._check_row_constraints(relname, r)
        self._check_db_constraints()

    def _check_row_constraint(self, relname, r, rw):
        row_validator = ' and '.join(
                           "({})".format(v)
                           for v in self.row_constraints[relname].values())
        if row_validator:
            with _dinsd.ns(self._system_ns.current):
                if not eval(row_validator, _all, rw._as_locals()):
                    # find first failing constraint to put in error message
                    for c, exp in sorted(self.row_constraints[relname].items()):
                        if not eval(exp, _all, rw._as_locals()):
                            raise RowConstraintError(relname, c, exp, rw)
                    raise AssertionError("Expected failure did not happen")

    def _check_row_constraints(self, relname, r):
        row_validator = ' and '.join(
                           "({})".format(v)
                           for v in self.row_constraints[relname].values())
        if row_validator:
            with _dinsd.ns(self._system_ns.current):
                invalid = r.where("not ({})".format(row_validator))
                if invalid:
                    # figure out one constraint and one row to put in error
                    # message; this is more useful than all of the constraints
                    # and all of the failed rows.
                    rw = sorted(invalid)[0]
                    for c, exp in sorted(self.row_constraints[relname].items()):
                        if not eval(exp, _all, rw._as_locals()):
                            raise RowConstraintError(relname, c, exp, rw)
                    raise AssertionError("Expected failure did not happen")

    def _check_db_constraints(self):
        for i in range(10):
            done = True
            for name, (constraint, fixer) in self._constraints.items():
                if callable(constraint):
                    valid = constraint()
                else:
                    valid = eval(constraint, _all, self._as_locals())
                if not valid and fixer is not None:
                    if callable(fixer):
                        valid = fixer()
                    else:
                        valid = eval(fixer, _all, self._as_locals())
                    if valid:
                        done = False
                if not valid:
                    raise DBConstraintError(name, constraint, fixer)
            if done:
                break
        else:
            raise DBConstraintLoop()

    def close(self):
        # Empty the dictionary and nullify the associated relations.  This does
        # two things: makes the relations inaccessible after a close, and
        # breaks the reference cycle between the Database object and the
        # relations.
        self._init()
        for r in self.values():
            r._rows = set()
            r.__class__ = DisconnectedPersistentRelation
        self.clear()

    # Row Constraints

    @_transaction_required
    def constrain_rows(self, relname, **kw):
        r = self[relname]
        existing = self.row_constraints[relname].copy()
        self.row_constraints[relname].update(kw)
        try:
            self._check_constraints(relname, r)
        except Exception:
            self.row_constraints[relname] = existing
            raise
        with self._con as con:
            con.add_row_constraints(relname, kw)

    def remove_row_constraints(self, relname, *args):
        self[relname]          # Key Error if no such rel.
        for arg in args:
            del self.row_constraints[relname][arg]
        with self._con as con:
            con.del_row_constraints(relname, args)

    # Key Constraints

    @_transaction_required
    def set_key(self, relname, keynames):
        r = self._transaction_ns.current[relname]
        r._validate_attr_names(keynames)
        k = self._system_ns.current['_sys_key_'+relname] = r >> keynames
        r.key = k.header
        # XXX I think these need to be transactionized, too.
        self.row_constraints[relname]['_sys_key_'+relname] = (
            "_row_ in {relname} or "
            "_row_ >> _sys_key_{relname}.header.keys() not in "
                "_sys_key_{relname}".format(relname=relname))
        self._constraints['_key_'+relname] = (
            "len({relname})==len(_sys_key_{relname})".format(relname=relname),
            lambda r=relname: self._update_key(r))

    def _update_key(self, relname):
        r = self._transaction_ns.current[relname]
        keyname = '_sys_key_'+relname
        key = self._system_ns.current[keyname]
        if len(r) == len(key):
            raise AssertionError("Relation and key have equal len in key fixer")
        # We could just re-project the key, but this helps us find bugs.
        if len(r) > len(key):
             new_key = key | (r - key) >> key.header.keys()
        else:
             new_key = _dinsd.matching(key, r)
        self._system_ns.current[keyname] = new_key
        return True

    def key(self, relname):
        return set(self._system_ns.current['_sys_key_'+relname].header.keys())



#
# Dumb persistence infrastructure using sqlite.
#

import sys
class _dumb_sqlite_persistence:

    def __init__(self, fn, debug_sql=sys.stderr):
        self.dbfn = fn
        self.debug_sql = debug_sql

    def new_con(self):
        con = _sqlite.connect(self.dbfn, isolation_level=None)
        if self.debug_sql:
            outfile = None if self.debug_sql is True else self.debug_sql
            con.set_trace_callback(lambda x: print(x, file=outfile))
        return _dumb_sqlite_connection(con)


class _dumb_sqlite_connection:

    def __init__(self, con):
        self.con = con

    def __enter__(self):
        self.con.cursor().execute("savepoint _dinsd")
        return self

    def __exit__(self, exc_type, exc_info, tb):
        if exc_type is None:
            self.con.cursor().execute("release _dinsd")
        else:
            self.con.cursor().execute("rollback to _dinsd")

    def initialize_sqlite_db_if_needed(self):
        c = self.con.cursor()
        c.execute('PRAGMA foreign_keys = ON')
        c.execute('create table if not exists "_relnames" ('
                    '"relname" varchar unique not null)')
        c.execute('create table if not exists "_reldefs" ('
                    '"relname" varchar not null '
                        'constraint "_reldefs_fkey" '
                            'references _relnames ("relname") '
                            'on delete cascade,'
                    '"attrname" varchar not null, '
                    '"attrtype" blob not null, '
                    'primary key ("relname", "attrname") '
                    ') ')
        c.execute('create table if not exists "_row_constraints" ('
                    '"relname" varchar not null '
                        'constraint "_row_constraints_fkey" '
                        'references _relnames ("relname") '
                        'on delete cascade,'
                    '"constraint_name" varchar, '
                    '"constraint" varchar,'
                    'primary key ("relname", "constraint_name")'
                    ') ')
        # This is a minor performance thing and not really required.
        c.execute('create index if not exists "_row_constraints_relname_index" '
                    'on "_row_constraints" ("relname")')

    def add_reltype(self, name, header):
        c = self.con.cursor()
        columns = ', '.join('"{}" blob'.format(n) for n in header)
        c.execute('create table "{}" ({})'.format(name, columns))
        c.execute('insert into "_relnames" ("relname") values (?)', (name,))
        for n, t in header.items():
            c.execute('insert into "_reldefs" ("relname", "attrname", "attrtype") '
                        'values (?, ?, ?)', (name, n, _pickle.dumps(t)))

    def update_relation(self, name, val):
        c = self.con.cursor()
        c.execute('delete from "{}"'.format(name))
        names = sorted(val.header.keys())
        for rw in val:
            c.execute('insert into "{}" ({}) values ({})'.format(
                            name,
                            ' ,'.join('"{}"'.format(n) for n in names),
                            ' ,'.join(['?'] * len(names))), 
                      [_pickle.dumps(getattr(rw, n)) for n in names])

    def insert_row(self, name, rw):
        c = self.con.cursor()
        names = sorted(rw._header_.keys())
        c.execute('insert into "{}" ({}) values ({})'.format(
            name,
            ' ,'.join('"{}"'.format(n) for n in names),
            ' ,'.join(['?'] * len(names))),
            [_pickle.dumps(getattr(rw, n)) for n in names])

    def update_row(self, name, key, changes):
        c = self.con.cursor()
        namebits, set_values = zip(*[('"{}"=?'.format(attrname), val)
                                     for attrname, val in changes.items()])
        setstr = ', '.join(namebits)
        namebits, where_values = zip(*[('"{}"=?'.format(attrname), val)
                                       for attrname, val in key.items()])
        wherestr = ' and '.join(namebits)
        # We assume the pickle of a given value is always the same.
        c.execute('update "{}" set {} where {}'.format(name, setstr, wherestr),
                        [_pickle.dumps(v) for v in set_values + where_values])

    def relations(self):
        c = self.con.cursor()
        c.execute('select "relname", "attrname", "attrtype" from "_reldefs"')
        headers = _collections.defaultdict(dict)
        for relname, attrname, attrtype in c:
            headers[relname][attrname] = _pickle.loads(attrtype)
        rels = []
        for relname, header in headers.items():
            r = _rel(**header)()
            c.execute('select * from "{}"'.format(relname))
            names = [t[0] for t in c.description]
            for rwdata in c:
                r._rows.add(r.row({n: _pickle.loads(v)
                                    for n, v in zip(names, rwdata)}))
            rels.append((relname, r))
        return rels

    def get_row_constraints(self):
        constraints = _collections.defaultdict(dict)
        c = self.con.cursor()
        c.execute('select "relname", "constraint_name", "constraint" '
                      'from _row_constraints')
        for relname, constraint_name, constraint in c:
            constraints[relname][constraint_name] = constraint
        return constraints

    def add_row_constraints(self, relname, constraints):
        c = self.con.cursor()
        for constraint_name, constraint in constraints.items():
            c.execute('insert into "_row_constraints" '
                          '("relname", "constraint_name", "constraint") '
                          'values (?, ?, ?)',
                       (relname, constraint_name, constraint))

    def del_row_constraints(self, relname, names):
        c = self.con.cursor()
        in_qs = ', '.join('?'*len(names))
        c.execute('delete from "_row_constraints" '
                      'where "relname"=? and '
                      '"constraint_name" in ({})'.format(in_qs),
                      (relname,) + names)


#Licensed under the Apache License, Version 2.0 (the "License");
#you may not use this file except in compliance with the License.
#You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
#Unless required by applicable law or agreed to in writing, software
#distributed under the License is distributed on an "AS IS" BASIS,
#WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#See the License for the specific language governing permissions and
#limitations under the License.
