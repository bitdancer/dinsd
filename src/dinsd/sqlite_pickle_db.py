#Copyright 2012, 2013 R. David Murray (see end comment for terms).
import collections as _collections
import contextlib as _contextlib
import weakref as _weakref
import pickle as _pickle
import sqlite3 as _sqlite
import threading as _threading
import dinsd as _dinsd
from dinsd import (rel as _rel, expression_namespace as _all, _Relation,
                   _hsig, display as _display)
from dinsd.db import (ConstraintError, RowConstraintError, DBConstraintLoop,
                      Rollback)
_null = _contextlib.ExitStack()

# For debugging only.
import sys as _sys
def ___(*args):
    print(*args, file=_sys.stderr, flush=True)
    return args[-1]
_all['___'] = ___


class PersistentRelation(_Relation):

    def __init__(self, db, name, r):
        self.db = db
        self.name = name
        super().__init__(r)

    def __str__(self):
        return self.display(*sorted(self.header))

    def display(self, *args, **kw):
        if 'highlight' not in kw:
            kw['highlight'] = getattr(self, 'key', [])
        return _display(self, *args, **kw)


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


class Database(dict):

    def __init__(self, fn, debug_sql=False):
        self.r = _R(self)
        storage = self._storage = _dumb_sqlite_persistence(fn, debug_sql=False)
        self._init()
        self.row_constraints.update(storage.get_row_constraints())
        for name, r in self._storage.relations():
            super().__setitem__(name, _get_persistent_type(r)(self, name, r))

    def _init(self):
        self.row_constraints = _collections.defaultdict(dict)
        self._system_relations = {}
        self._system_ns = _dinsd._NS(self._system_relations)
        self._constraints = {}
        self._transaction_ns = _dinsd._NS(self, in_getitem=False)

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
            self._update_db_rels(changes)
            self._system_relations.update(system_changes)

    @property
    def transactions(self):
        return len(self._transaction_ns) - 1

    def _update_db_rels(self, updated_rels):
        with self._storage.transaction():
            for name, val in updated_rels.items():
                oldval = self.get(name)
                if oldval is None:
                    self._storage.add_reltype(name, val.header)
                if val != oldval:
                    self._storage.update_relation(name, val)
        for name, val in updated_rels.items():
            # XXX this can be made more efficient.
            super().__setitem__(name, _get_persistent_type(val)(self, name, val))

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
        with _null if self.transactions else self.transaction():
            self._transaction_ns.current[name] = val

    def __getitem__(self, name):
        # XXX I wonder if there is a more elegant way to to do this.
        if self._transaction_ns.in_getitem:
            self._transaction_ns.in_getitem = False
            return super().__getitem__(name)
        self._transaction_ns.in_getitem = True
        return self._transaction_ns.current[name]

    def __repr__(self):
        return "{}({{{}}})".format(
            self.__class__.__name__,
            ', '.join("{!r}: {!r}".format(n, type(r))
                      for n, r in sorted(self.items())))

    def _check_constraints(self, relname, r):
        row_validator = ' and '.join(
                           "({})".format(v)
                           for v in self.row_constraints[relname].values())
        if row_validator:
            # We need a transaction here to get the db relation names in scope.
            with (_null if self.transactions else
                    self.transaction()), _dinsd.ns(self._system_ns.current):
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
            r._rows_ = set()
            r.__class__ = DisconnectedPersistentRelation
        self.clear()

    # Row Constraints

    def constrain_rows(self, relname, **kw):
        r = self[relname]
        existing = self.row_constraints[relname].copy()
        self.row_constraints[relname].update(kw)
        try:
            self._check_constraints(relname, r)
        except Exception:
            self.row_constraints[relname] = existing
            raise
        self._storage.add_row_constraints(relname, kw)

    def remove_row_constraints(self, relname, *args):
        self[relname]          # Key Error if no such rel.
        for arg in args:
            del self.row_constraints[relname][arg]
        self._storage.del_row_constraints(relname, args)

    # Key Constraints

    def set_key(self, relname, keynames):
        r = self[relname]
        r._validate_attr_names(keynames)
        k = self._system_ns.current['_sys_key_'+relname] = r >> keynames
        r.key = k.header
        self.row_constraints[relname]['_sys_key_'+relname] = (
            "_row_ in {relname} or "
            "_row_ >> _sys_key_{relname}.header.keys() not in "
                "_sys_key_{relname}".format(relname=relname))
        self._constraints['_key_'+relname] = (
            "len({relname})==len(_sys_key_{relname})".format(relname=relname),
            lambda r=relname: self._update_key(r))

    def _update_key(self, relname):
        r = self[relname]
        keyname = '_sys_key_'+relname
        key = self._system_ns.current[keyname]
        if len(r) == len(key):
            raise AssertionError("Relation and key have equal len in key fixer")
        # We could just re-project the key, but this is more fun...for now.
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

class _dumb_sqlite_persistence:

    def __init__(self, fn, debug_sql=False):
        con = self.con = _sqlite.connect(fn)
        if debug_sql:
            outfile = None if debug_sql is True else debug_sql
            con.set_trace_callback(lambda x: print(x, file=outfile))
        c = con.cursor()
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
        # This is a performance thing and not really required.
        c.execute('create index if not exists "_row_constraints_relname_index" '
                    'on "_row_constraints" ("relname")')

    def transaction(self):
        # The connection is a context manager for sqlite transactions.
        return self.con

    def add_reltype(self, name, header):
        db = self.con
        c = db.cursor()
        columns = ', '.join('"{}" blob'.format(n) for n in header)
        c.execute('create table "{}" ({})'.format(name, columns))
        c.execute('insert into "_relnames" ("relname") values (?)', (name,))
        for n, t in header.items():
            c.execute('insert into "_reldefs" ("relname", "attrname", "attrtype") '
                        'values (?, ?, ?)', (name, n, _pickle.dumps(t)))
        db.commit()

    def update_relation(self, name, val):
        db = self.con
        c = db.cursor()
        c.execute('delete from "{}"'.format(name))
        names = sorted(val.header.keys())
        for rw in val:
            c.execute('insert into "{}" ({}) values ({})'.format(
                            name,
                            ' ,'.join('"{}"'.format(n) for n in names),
                            ' ,'.join(['?'] * len(names))), 
                      [_pickle.dumps(getattr(rw, n)) for n in names])
        db.commit()

    def relations(self):
        db = self.con
        c = db.cursor()
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
                r._rows_.add(r.row({n: _pickle.loads(v)
                                    for n, v in zip(names, rwdata)}))
            rels.append((relname, r))
        return rels

    def get_row_constraints(self):
        db = self.con
        constraints = _collections.defaultdict(dict)
        c = db.cursor()
        c.execute('select "relname", "constraint_name", "constraint" '
                    'from _row_constraints')
        for relname, constraint_name, constraint in c:
            constraints[relname][constraint_name] = constraint
        return constraints

    def add_row_constraints(self, relname, constraints):
        db = self.con
        c = db.cursor()
        for constraint_name, constraint in constraints.items():
            db.execute('insert into "_row_constraints" '
                          '("relname", "constraint_name", "constraint") '
                          'values (?, ?, ?)',
                       (relname, constraint_name, constraint))
        db.commit()

    def del_row_constraints(self, relname, names):
        db = self.con
        c = db.cursor()
        in_qs = ', '.join('?'*len(names))
        c.execute('delete from "_row_constraints" '
                    'where "relname"=? and '
                    '"constraint_name" in ({})'.format(in_qs),
                  (relname,) + names)
        db.commit()


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
