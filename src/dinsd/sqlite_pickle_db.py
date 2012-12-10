#Copyright 2012 R. David Murray (see end comment for terms).
import collections as _collections
import pickle as _pickle
import sqlite3 as _sqlite
from dinsd import rel as _rel, expression_namespace as _all
from dinsd.db import ConstraintError, RowConstraintError

# For debugging only.
import sys as _sys
#__ = lambda *args: print(*args, file=_sys.stderr)


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
        for attrname, val in self._storage.relations():
            val._db = self
            val.__name__ = attrname
            super().__setitem__(attrname, val)

    def _init(self):
        self.row_constraints = _collections.defaultdict(dict)
        self._constraint_ns = _collections.ChainMap(_all)
        self._constraints = {}

    def __setitem__(self, name, val):
        attr = self.get(name)
        if attr is not None:
            if type(val) != type(attr):
                raise ValueError("Cannot assign value of type {} to "
                    "attribute of type {}".format(type(val), type(attr)))
        else:
            if isinstance(val, type):
                val = val()
            if not hasattr(val, 'header'):
                raise ValueError("Database attributes must be relations, "
                    "not {}".format(type(val)))
            self._storage.add_reltype(name, type(val))
        self._check_constraints(name, val)
        val._db = self
        val.__name__ = name
        self._storage.update_relation(name, val)
        super().__setitem__(name, val)

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
            invalid = r.where("not ({})".format(row_validator))
            if invalid:
                # figure out one constraint and one row to put in error message;
                # this is more useful than all the constraints and failures.
                rw = sorted(invalid)[0]
                for c, exp in sorted(self.row_constraints[relname].items()):
                    if not eval(exp, rw._as_locals(), _all):
                        raise RowConstraintError(relname, c, exp, rw)
                raise AssertionError("Expected failure did not happen")
        for i in range(10):
            done = True
            for name, (constraint, fixer) in self._constraints.items():
                if callable(constraint):
                    valid = constraint()
                else:
                    valid = eval(constraint, self._as_locals(), _all)
                if not valid and fixer is not None:
                    if callable(fixer):
                        valid = fixer()
                    else:
                        valid = eval(constraint, {}, self._constraint_ns)
                if not valid:
                    raise DBConstraintError(name, constraint, fixer)
                else:
                    done = False
            if done:
                break
        else:
            raise DBConstraintLoop()

    def close(self):
        # Empty the dictionary.  This does two things: makes the relations
        # inaccessible after a close, and breaks the reference cycle between
        # the Database object and the relations.
        self.clear()
        self._init()

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
        getattr(self.r, relname)          # Attribute Error if no such rel.
        for arg in args:
            del self.row_constraints[relname][arg]
        self._storage.del_row_constraints(relname, args)

    # Key Constraints

    def set_key(self, relname, keynames):
        r = getattr(self.r, relname)
        r._validate_attr_names(keynames)
        self._constraint_ns['_key_'+relname] = r >> keynames
        self._constraints['_key_'+relname] = (
            "len(relname)==len(_key_relaname)",
            lambda r=relname: self._update_key(r))
        self.row_constraints[relname]['_key_'+relname] = (
            "_row_ >> _key_{}.header.keys() not in _key_{}".format(
                relname, relname))

    def _update_key(self, relname):
        r = getattr(self.r, relname)
        key = self._keys[relname]
        if len(r) < len(key):
            self._keys[relname] = key | (r - key) >> key.header.keys()
        else:
            self._keys[relname] = matching(key, r)
        return True

    def key(self, relname):
        return set(self._constraint_ns['_key_'+relname].header.keys())



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


    def add_reltype(self, name, reltype):
        db = self.con
        header = reltype.header
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
