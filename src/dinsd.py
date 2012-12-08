#Copyright 2012 R. David Murray (see end comment for terms).

import collections as _collections
import contextlib as _contextlib
import itertools as _itertools
import operator as _operator
import pickle as _pickle
import sqlite3 as _sqlite
import threading as _threading
import types as _types
import weakref as _weakref

# For debugging only.
import sys as _sys
dbg = lambda *args: print(*args, file=_sys.stderr)



#
# Total Ordering infrastructure
#


class _RichCompareMixin:

    # Flexible rich compare adapted from recipe by Lenart Regbro.

    def _compare(self, other, method):
        try:
            return method(self._cmpkey(), other._cmpkey())
        except TypeError:
            # _cmpkey returned an incommensurate type.
            return NotImplemented

    def __lt__(self, other):
        return self._compare(other, lambda s,o: s < o)

    def __le__(self, other):
        return self._compare(other, lambda s,o: s <= o)

    def __eq__(self, other):
        return self._compare(other, lambda s,o: s == o)

    def __ge__(self, other):
        return self._compare(other, lambda s,o: s >= o)

    def __gt__(self, other):
        return self._compare(other, lambda s,o: s > o)

    def __ne__(self, other):
        return self._compare(other, lambda s,o: s != o)

    def __hash__(self):
        return hash(self._cmpkey())



#
# User Defined type support
#


class Scaler(_RichCompareMixin):

    def _cmpkey(self):
        return self.value

    def _compare(self, other, method):
        if type(self) != type(other):
            return NotImplemented
        return super()._compare(other, method)

    def __str__(self):
        return str(self.value)

    def __repr__(self):
        return "{}({!r})".format(self.__class__.__name__, self.value)



#
# Relation Type Metatype
#

class _RelationTypeMeta(type):

    @property
    def header(self):
        return self._header.copy()

    @property
    def degree(self):
        return len(self._header)



#
# Row types (TTM/Tutorial D TUPLE Types)
#


def row(*args, **kw):
    if len(args)==1:
        kw = dict(args[0], **kw)
    header = {}
    for n, v in sorted(kw.items()):
        if n.startswith('_'):
            raise ValueError("Invalid relational attribute name "
                             "{!r}".format(n))
        if isinstance(v, type):
            raise ValueError('Invalid value for attribute {!r}: '
                             '"{!r}" is a type'.format(n, v))
        header[n] = attrtype = type(v)
    cls = _get_type('row', header)
    return cls(kw)


def _row_dct(header):
    return {'_header': header}


class _Row(_RichCompareMixin, metaclass=_RelationTypeMeta):

    _header = {}

    def __init__(self, *args, **kw):
        if len(args) > 1:
            raise TypeError("row() takes at most one positional argument "
                            "({} given)".format(len(args)))
        if args and not kw and hasattr(args[0], '_header_'):
            # We are being called as a type function.
            arg = args[0]
            if self._header_ != arg._header_:
                raise TypeError("Invalid Row type: {!r}".format(arg))
            self.__dict__ = arg.__dict__
            return
        attrdict = kw
        if args:
            kw.update(args[0])
        if len(attrdict) != self._degree_:
            raise TypeError("Expected {} attributes, got {} ({!r})".format(
                                self._degree_, len(attrdict), attrdict))
        for attr, value in attrdict.items():
            try:
                setattr(self, attr, self._header_[attr](value))
            except (TypeError, ValueError) as e:
                raise type(e)(str(e) + "; {!r} invalid for attribute {}".format(
                                value, attr))
            except KeyError:
                raise TypeError(
                    "Invalid attribute name {}".format(attr)) from None

    # Access to class properties

    @property
    def _header_(self):
        return self.__class__.header

    @property
    def _degree_(self):
        return self.__class__.degree

    # Miscellaneous operators.

    def __iter__(self):
        return iter(self.__dict__)

    def __len__(self):
        return len(self.__dict__)

    # Comparison operators (see RichCompareMixin).

    def _cmpkey(self):
        return tuple(sorted(self.__dict__.items()))

    def _compare(self, other, method):
        if not isinstance(other, _Row) or self._header_ != other._header_:
            return NotImplemented
        return super()._compare(other, method)

    def __eq__(self, other):
        if not isinstance(other, _Row):
            return False
        return self._cmpkey() == other._cmpkey()

    def __ne__(self, other):
        return not self == other

    def __hash__(self):
        return hash(self._cmpkey())

    # Infix relational operators.

    def __and__(self, other):                   # &
        return ~(rel(self) & rel(other))

    def __add__(self, other):                   # +
        return ~(rel(self) + rel(other))

    def __rshift__(self, attrnames):            # >>
        return ~(rel(self) >> attrnames)

    def __lshift__(self, attrnames):            # <<
        return ~(rel(self) << attrnames)

    def __invert__(self):                       # ~
        return rel(self)

    # Postfix relational operators.

    def rename(self, **kw):
        return ~(rel(self).rename(**kw))

    def extend(self, **kw):
        return ~(rel(self).extend(**kw))

    # Presentation operators.

    def __repr__(self):
        return "row({{{}}})".format(
            ', '.join("{!r}: {!r}".format(k, v)
                        for k, v in sorted(self.__dict__.items())))

    def __str__(self):
        return '{{{}}}'.format(
            ', '.join('{}={}'.format(k, v)
                        for k, v in sorted(self.__dict__.items())))

    # Internal methods.

    def _as_locals(self):
        l = self.__dict__.copy()
        l['_row_'] = self
        l.update(_locals[_threading.current_thread].__dict__)
        return l



#
# Relation types
#


def rel(*args, **kw):
    # For reasons that derive from trying to imitate Tutorial D and may not be
    # the best idea, there are two overloaded cases here:  defining a type, and
    # relation literals.  Type definition is characterized by a single,
    # possibly empty, dictionary argument whose values are all types, possibly
    # with additional keyword arguments whose values are types. A relation
    # literal is everything else: an iterable or a list of arguments consisting
    # of dictionary and/or Row objects all of the same type: the type of the
    # literal is determined by the type of the first item in the iterator or
    # the first argument in the argument list.  If there are no arguments or
    # keywords, or a single argument that is an empty iterator, then we have a
    # relation literal representing Dum.
    body = None
    if (not args and kw or len(args) == 1 and hasattr(args[0], 'values') and
            all(isinstance(v, type) for v in args[0].values())):
        # Relation type declaration.
        header = args[0].copy() if args else {}
        header.update(kw)
        for n, v in sorted(header.items()):
            if n.startswith('_'):
                raise ValueError("Invalid relational attribute name "
                                 "{!r}".format(n))
            if not isinstance(v, type):
                raise ValueError(
                    'Invalid value for attribute {!r} in relation type '
                    'definition: "{!r}" is not a type'.format(n, v))
    else:
        # Relation literal form.
        if kw:
            raise TypeError("keywords attributes not valid in relation "
                            "literal form of rel call")
        if (len(args) == 1 and not hasattr(args[0], 'items') and
                               not hasattr(args[0], '_header_')):
            # Single argument iterator
            args = args[0]
        iterable = iter(args)
        try:
            r = next(iterable)
        except StopIteration:
            # Empty iterator == relation literal for Dum.
            header = {}
            body = []
        else:
            if not hasattr(r, '_header_'):
                r = row(r)
            header = r._header_.copy()
            body = _itertools.chain([r], iterable)
    new_rel = _get_type('rel', header)
    return new_rel if body is None else new_rel(body)


def _rel_dct(header):
    return dict(_header=header, row=_get_type('row', header))


def _rel(attrdict):
    # For internal use we don't need to do all those checks above.
    return _get_type('rel', attrdict.copy())


class _Relation(_RichCompareMixin, metaclass=_RelationTypeMeta):

    _header = {}

    def __init__(self, *args):
        # Several cases: (1) empty relation (2) being called as a type
        # validation function (single arg is a Relation) (3) header tuple
        # followed by value tuples (4) iterable of dict-likes and/or Rows,
        # which could be a single argument or could be one dict/Row per arg.
        if len(args) == 0:
            # (1) Empty relation.
            self._rows_ = set()
            return
        if (len(args)==1 and isinstance(args[0], _Relation) and
                args[0].header == self.header):
            # (2) We were called as a type validation function.  Return an
            # immutable copy, because the only time this happens is when a
            # relation is the value of an attribute, and when a relation is a
            # value of an attribute it must be immutable.
            self._rows_ = frozenset(args[0]._rows_)
            return
        rows = []
        first = None
        if (not hasattr(args[0], 'items') and
                not hasattr(args[0], 'union') and
                not hasattr(args[0], '_header_') and
                hasattr(args[0], '__iter__') and
                hasattr(args[0], '__len__') and
                len(args[0]) and
                isinstance(next(iter(args[0])), str)):
            # (3) Tuple form.  Tuple form is a special shorthand, and we only
            # allow it in argument list form, not single-iterator-argument
            # form.  Furthermore, while the list of arguments could be sourced
            # from a generator (not that that would be a good idea), the
            # arguments themselves cannot be.
            attrlist = args[0]
            self._validate_attr_list(attrlist)
            for i, o in enumerate(args[1:], start=1):
                if len(o) != self.degree:
                    raise TypeError("Expected {} attributes, got {} in row {} "
                                    "for {}".format(
                                        self.degree,
                                        len(o),
                                        i,
                                        repr(self.__class__)))
                try:
                    rows.append(self.row({k: v for k, v in zip(attrlist, o)}))
                except TypeError as e:
                    raise TypeError(str(e) + " in row {}".format(i))
        else:
            # (4) iterator of dicts and/or Rows.
            if len(args) == 1 and not (hasattr(args[0], 'items') or
                                       hasattr(args[0], '_header_')):
                # Single iterator argument form.
                args = args[0]
            for i, o in enumerate(args):
                if hasattr(o, '_header_'):
                    # This one is a row.
                    if o._header_ != self.header:
                        raise TypeError("Row header does not match relation header "
                                        "in row {} (got {!r} for {!r})".format(
                                            i, o, type(self)))
                else:
                    # This one is a dict, turn it into a row.
                    try:
                        o = self.row(o)
                    except (ValueError, TypeError) as e:
                        raise type(e)(str(e) + " in row {}".format(i))
                rows.append(o)
        # One way or another we now have a list of Row objects.
        rowset = set()
        for i, r in enumerate(rows):
            if r in rowset:
                raise ValueError(
                    "Duplicate row: {!r} in row {} of input".format(r, i))
            rowset.add(r)
        self._rows_ = rowset

    def _validate_attr_list(self, attrlist):
        if len(attrlist) != self.degree:
            raise TypeError("Expected {} attributes, got {} ({}) in "
                            "header row for {}".format(
                                self.degree,
                                len(attrlist), sorted(attrlist),
                                repr(self.__class__)))
        self._validate_attr_names(attrlist)

    def _validate_attr_names(self, attrlist):
        for attr in attrlist:
            if attr not in self._header:
                raise AttributeError(
                    "{!r} has no attribute {!r}".format(self.__class__, attr))

    # Access to class properties

    @property
    def header(self):
        return self.__class__.header

    @property
    def degree(self):
        return self.__class__.degree

    # Miscellaneous operators.

    def __iter__(self):
        return iter(self._rows_)

    def __len__(self):
        return len(self._rows_)

    # Comparison operators (see RichCompareMixin).

    def _cmpkey(self):
        return self._rows_

    def _compare(self, other, method):
        if type(self) != type(other):
            return NotImplemented
        return super()._compare(other, method)

    def __eq__(self, other):
        if type(self) != type(other):
            return False
        return self._rows_ == other._rows_

    def __ne__(self, other):
        return not self == other

    def __hash__(self):
        return hash(self._rows_)

    # Infix relational operators.

    def __and__(self, other):                   # &
        return _binary_join(self, other)

    def __rshift__(self, attrnames):            # >>
        return project(self, attrnames)

    def __lshift__(self, attrnames):            # <<
        return project(self, all_but(attrnames))

    def __or__(self, other):                    # |
        return union(self, other)

    def __add__(self, other):                   # +
        return compose(self, other)

    def __sub__(self, other):                   # -
        return notmatching(self, other)

    def __invert__(self):
        return extract_only_row(self)           # ~

    # Postfix relational operators.

    def rename(self, **kw):
        return rename(self, **kw)

    def where(self, condition):
        return where(self, condition)

    def extend(self, **kw):
        return extend(self, **kw)

    def compute(self, expr):
        return compute(self, expr)

    def summarize(self, attrs, **kw):
        # If attrs is a relation this will work, but doing that is not part of
        # the official API and should not be depended on.
        return summarize(self, attrs, **kw)

    def group(self, **kw):
        return group(self, **kw)

    def ungroup(self, attrname):
        return ungroup(self, attrname)

    def wrap(self, **kw):
        return wrap(self, **kw)

    def unwrap(self, attrname):
        return unwrap(self, attrname)

    # Presentation operators.

    def __repr__(self):
        names = sorted(self.header)
        if self._rows_:
            return "rel({{{}}})".format(
                ', '.join(repr(row)
                          for row in sorted(self._rows_)))
        return self.__class__.__name__ + '()'

    def __str__(self):
        return _display(self, *sorted(self.header))



#
# Type registry
#

_type_registry = {_Relation: _weakref.WeakValueDictionary(),
                  _Row: _weakref.WeakValueDictionary()}
_typetype_map = {'rel': (_Relation, _rel_dct),
                 'row': (_Row, _row_dct)}
def _get_type(typetype, header):
    baseclass, dct_maker = _typetype_map[typetype]
    hsig = '_'.join(n+'-'+v.__name__+str(id(v))
                    for n, v in sorted(header.items()))
    cls = _type_registry[baseclass].get(hsig)
    if cls is None:
        dct = dct_maker(header)
        name = '{}({{{}}})'.format(
            typetype,
            ', '.join(repr(n)+': '+v.__name__
                      for n, v in sorted(header.items())))
        cls = type(name, (baseclass,), dct)
        _type_registry[baseclass][hsig] = cls
    return cls



#
# Relational Operators
#


Dum = rel()
Dee = rel(row())


def join(*relations):
    if not relations:
        return Dee
    if len(relations)==1 and not isinstance(relations[0], _Relation):
        # Assume it is an iterator.
        relations = relations[0]
    joined, *relations = relations
    for i, rel in enumerate(relations, start=1):
        try:
            joined = _binary_join(joined, rel)
        except TypeError as e:
            raise TypeError(str(e) + " (error detected while processing "
                            "argument {})".format(i))
    return joined


def _binary_join(first, second):
    combined_attrs = first.header.copy()
    common_attrs = []
    for attr, typ in second.header.items():
        if attr in combined_attrs:
            if typ != combined_attrs[attr]:
                raise TypeError("Duplicate attribute name ({!r}) "
                    "with different type (first: {}, second: {} found "
                    "in joined relations with types {} and {}".format(
                        attr,
                        combined_attrs[attr],
                        typ,
                        type(first),
                        type(second),
                        ))
            common_attrs.append(attr)
        else:
            combined_attrs[attr] = typ
    if common_attrs:
        # Build index for the match columns.
        getter = _operator.attrgetter(*common_attrs)
        index = _collections.defaultdict(list)
        for row in second:
            index[getter(row)].append(row)
        matches = lambda key: index[key]
    else:
        getter = lambda row: None
        matches = lambda key: second._rows_
    # Create an initially empty new relation of the new type, and then extend
    # it with the joined data.  Because the body is a set we don't have to
    # worry about duplicates.
    new_rel = _rel(combined_attrs)()
    for row in first._rows_:
        key = getter(row)
        for row2 in matches(key):
            attrs = vars(row).copy()
            attrs.update(vars(row2))
            new_rel._rows_.add(new_rel.row(attrs))
    return new_rel


def intersect(*relations):
    if not relations:
        return Dee
    if len(relations)==1 and not isinstance(relations[0], _Relation):
        # Assume it is an iterator.
        relations = relations[0]
    first, *relations = relations
    new_rel = type(first)()
    new_rel._rows_ = first._rows_
    for rel in relations:
        if first.header != rel.header:
            raise TypeError("Cannot take intersection of unlike relations")
        new_rel._rows_ = new_rel._rows_.intersection(rel._rows_)
    return new_rel


def times(*relations):
    if not relations:
        return Dee
    if len(relations)==1 and not isinstance(relations[0], _Relation):
        # Assume it is an iterator.
        relations = relations[0]
    first, *relations = relations
    for rel in relations:
        if first.header.keys() & rel.header.keys():
            raise TypeError("Cannot multiply relations that share attributes")
    return join(first, *relations)


def rename(relation, **renames):
    new_attrs = relation.header.copy()
    holder = {}
    for old, new in renames.items():
        if new.startswith('_'):
            raise ValueError("Invalid relational attribute name "
                             "{!r}".format(new))
        if new in holder:
            raise ValueError("Duplicate relational attribute name "
                             "{!r}".format(new))
        holder[new] = new_attrs.pop(old)
    new_attrs.update(holder)
    new_rel = _rel(new_attrs)()
    for row in relation._rows_:
        row_data = vars(row).copy()
        holder = {}
        for old, new in renames.items():
            holder[new] = row_data.pop(old)
        row_data.update(holder)
        new_rel._rows_.add(new_rel.row(row_data))
    return new_rel


class all_but:

    def __init__(self, attr_names):
        self.names = attr_names

    def all_but(self, relation):
        if type(self.names) == type(self):
            # negation of a negation is a positive...give back original names.
            # We only handle one level of recursion...perhaps this whole
            # interface implementation should be rethought.
            return self.names.names
        all_names = relation.header.keys()
        if self.names and all_names & self.names != self.names:
            raise TypeError("Attribute list included unknown attributes: "
                            "{}".format(self.names - all_names))
        return all_names - self.names


def project(relation, attr_names):
    if hasattr(attr_names, 'all_but'):
        attr_names = attr_names.all_but(relation)
    reduced_attrs = {n: t for n, t in relation.header.items()
                          if n in attr_names}
    if not len(reduced_attrs) == len(attr_names):
        raise TypeError("Attribute list included unknown attributes: "
                        "{}".format(attr_names - reduced_attrs.keys()))
    reduced_attr_names = reduced_attrs.keys()
    new_rel = _rel(reduced_attrs)()
    for row in relation._rows_:
        new_row_data = {n: v for n, v in vars(row).items()
                             if n in reduced_attr_names}
        new_rel._rows_.add(new_rel.row(new_row_data))
    return new_rel


def where(relation, condition):
    if isinstance(condition, str):
        condition = lambda r, s=condition: eval(s, _all, r._as_locals())
    new_rel = type(relation)()
    for row in relation._rows_:
        if condition(row):
            new_rel._rows_.add(row)
    return new_rel


def extend(relation, _name_check=True, **new_attrs):
    if len(relation) == 0:
        # Tutorial D can do this, but the fact that we can't probably doesn't
        # matter much in practice.
        raise TypeError("Cannot extend empty relation")
    for n, f in new_attrs.items():
        if _name_check and n.startswith('_'):
            raise ValueError("Invalid relational attribute name "
                             "{!r}".format(n))
        if n in relation.header:
            raise ValueError("Duplicate relational attribute name "
                             "{!r}".format(n))
        if isinstance(f, str):
            new_attrs[n] = lambda r, s=f: eval(s, _all, r._as_locals())
    attrs = relation.header
    row1 = next(iter(relation))
    attrs.update({n: type(new_attrs[n](row1)) for n in new_attrs.keys()})
    new_rel = _rel(attrs)()
    for row in relation:
        new_values = vars(row).copy()
        new_values.update({n: new_attrs[n](row) for n in new_attrs.keys()})
        new_rel._rows_.add(new_rel.row(new_values))
    return new_rel


def union(*relations):
    if len(relations) == 0:
        return Dum
    if len(relations)==1 and not isinstance(relations[0], _Relation):
        # Assume it is an iterator.
        relations = relations[0]
    first, *relations = relations
    new_rel = type(first)()
    new_rel._rows_.update(first._rows_.copy())
    for rel in relations:
        if not first.header == rel.header:
            raise TypeError("Union operands must of equal types")
        new_rel._rows_.update(rel._rows_.copy())
    return new_rel


# XXX: this should probably be a public API of some sort.
def _common_attrs(first, second):
    common_attrs = set()
    for attr, typ in second.header.items():
        if attr in first.header:
            if typ != first.header[attr]:
                raise TypeError("Duplicate attribute name ({!r}) "
                    "with different type (first: {}, second: {} "
                    "found in match relation (relation types "
                    "are {} and {})".format(
                        attr,
                        first.header[attr],
                        typ,
                        type(first),
                        type(second),
                        ))
            common_attrs.add(attr)
    return common_attrs


def _matcher(first, second, match):
    common_attrs = _common_attrs(first, second)
    new_rel = type(first)()
    if not common_attrs:
        if bool(second) == match:   # exclusive or
            new_rel._rows_.update(first._rows_)
        return new_rel
    getter = _operator.attrgetter(*common_attrs)
    index = set()
    for row in second:
        index.add(getter(row))
    for row in first._rows_:
        if (getter(row) in index) == match:
            new_rel._rows_.add(row)
    return new_rel


def notmatching(first, second):
    return _matcher(first, second, match=False)


def minus(first, second):
    if not first.header == second.header:
        raise TypeError("Relation types must match for minus operation")
    return notmatching(first, second)


def matching(first, second):
    return _matcher(first, second, match=True)


def compose(first, second):
    common_attrs = _common_attrs(first, second)
    return project(join(first, second), all_but(common_attrs))



#
# display machinery
#


def display(relation, *columns, **kw):
    relation._validate_attr_list(columns)
    return _display(relation, *columns, **kw)


def _display(relation, *columns, sort=[]):
    toprint = [list(map(_printable, columns))]
    getter = _operator.attrgetter(*columns) if columns else lambda x: x
    # Working around a little Python wart here.
    if len(columns) == 1:
        rows = [(_printable(getter(row)),) for row in relation._rows_]
    else:
        rows = [list(map(_printable, getter(row))) for row in relation._rows_]
    tosort = [sort] if isinstance(sort, str) else sort
    if not tosort:
        tosort = columns
    indexes = []
    for c in tosort:
        indexes.append(columns.index(c))
    sortgetter = _operator.itemgetter(*indexes) if indexes else None
    toprint.extend(sorted(rows, key=sortgetter))
    widths = [max([x.width for x in vals]) for vals in zip(*toprint)]
    sep = '+' + '+'.join(['-'*(w+2) for w in widths]) + '+'
    r = [sep]
    r.extend((_tline(parts, widths) for parts in zip(*toprint[0]))
             if columns else ['||'])
    # XXX This if test is ugly, improve it.
    if (hasattr(relation, '_db') and
            '_key_'+relation.__name__ in relation._db._constraint_ns):
        h = ['+']
        key = relation._db.key(relation.__name__)
        for i, name in enumerate(columns):
            if name in key:
                h.append('=' * (widths[i]+2))
            else:
                h.append('-' * (widths[i]+2))
            h.append('+')
        r.append(''.join(h))
    else:
        r.append(sep)
    if not columns and len(toprint)==2:
        r.append("||")
    else:
        for row in toprint[1:]:
            r.extend(_tline(parts, widths)
                     for parts in _itertools.zip_longest(*row))
    r.append(sep)
    return '\n'.join(r)


class _printable(_RichCompareMixin):

    def __init__(self, content):
        self.source = content
        content = str(content)
        if '\n' in content:
            self.content = content.splitlines() + ['']
            self.width = max(map(len, self.content))
        else:
            self.width = len(content)
            self.content = [content]

    def _cmpkey(self):
        return self.content

    def __iter__(self):
        for line in self.content:
            yield line

    def __repr__(self):
        return "_printable({!r})".format(self.source)


def _tline(parts, widths):
    return ('| ' + ' | '.join(('' if v is None else v).ljust(w)
                                  for v, w in zip(parts, widths)) + ' |')



#
# Aggregate Operators
#


def compute(relation, expr):
    if isinstance(expr, str):
        expr = lambda r, s=expr: eval(s, _all, r._as_locals())
    for row in relation:
        yield expr(row)


def avg(iterator):
    """Calculate the average of an iterator.

    This is a functional, iterative implementation, which means the iterator
    does not have to support len, and we accumulate the result as we go along
    rather than first exhausting the iterator and then performing the
    summation.
    """
    c = 0
    for s, c in _itertools.accumulate(zip(iterator, _itertools.repeat(1)),
                                     lambda x, y: (x[0]+y[0], x[1]+y[1])):
        pass
    return 0/0 if c==0 else s/c



#
# Extended Operators
#


def summarize(relation, comprel, _debug_=False, **new_attrs):
    if not isinstance(comprel, _Relation):
        # Assume it is an attribute name list
        comprel = relation >> comprel
    x = extend(comprel, _name_check=False,
               _summary_=lambda r, t=type(comprel): compose(relation, t(r)))
    if _debug_:
        print(x)
    return extend(x, **new_attrs) << {'_summary_'}


def group(relation, **kw):
    if len(kw) > 1:
        raise TypeError("Only one new attribute may be specified for group")
    name, attr_names = next(iter(kw.items()))
    grouped = relation << attr_names
    grouping_func = lambda r: compose(relation, type(grouped)(r))
    return extend(grouped, **{name: grouping_func})


def ungroup(relation, attrname):
    if not(relation):
        raise ValueError("Cannot ungroup an empty relation")
    attrs = relation.header.copy()
    row1 = next(iter(relation))
    del attrs[attrname]
    attrs.update(getattr(row1, attrname).header)
    new_rel = _rel(attrs)()
    for row in relation:
        new_values = vars(row).copy()
        subrel = new_values.pop(attrname)
        for subrow in subrel:
            new_values.update(vars(subrow))
            new_rel._rows_.add(new_rel.row(new_values))
    return new_rel

    
def wrap(relation, **kw):
    if len(kw) > 1:
        raise TypeError("Only one new attribute may be specified for wrap")
    name, attr_names = next(iter(kw.items()))
    if hasattr(attr_names, 'all_but'):
        attr_names = attr_names.all_but(relation)
    sub_rel = type(relation >> attr_names)
    row_func = lambda r: sub_rel.row({n: getattr(r, n) for n in attr_names})
    return extend(relation, **{name: row_func}) << attr_names


def unwrap(relation, attrname):
    if not(relation):
        raise ValueError("Cannot unwrap an empty relation")
    attrs = relation.header.copy()
    row1 = next(iter(relation))
    del attrs[attrname]
    attrs.update(getattr(row1, attrname)._header_)
    new_rel = _rel(attrs)()
    for row in relation:
        new_values = vars(row).copy()
        subrow = new_values.pop(attrname)
        new_values.update(vars(subrow))
        new_rel._rows_.add(new_rel.row(new_values))
    return new_rel


def extract_only_row(relation):
    if len(relation) > 1:
        raise ValueError(
            "{} object has more than one row".format(type(relation)))
    return next(iter(relation))



#
# Namespace management
#


# The bulk of the public API.
_names = {x for x in globals() if not x.startswith('_')}

# Expression global namespace.
_all = {n: v for n, v in globals().items() if n in _names}
expression_namespace = _all


# 'with ns()' support.
_locals = _collections.defaultdict(_types.SimpleNamespace)

@_contextlib.contextmanager
def ns(*args, **kw):
    ns = _threading.local()
    ns.__dict__.update(*args, **kw)
    _locals[_threading.current_thread] = ns
    yield ns
    del _locals[_threading.current_thread]



#
# Databases
#


class ConstraintError(Exception):
    pass


class RowConstraintError(ConstraintError):

    def __init__(self, relname, cname, constraint, invalid):
        self.relname = relname
        self.cname = cname
        self.constraint = constraint
        self.invalid = invalid

    def __str__(self):
        return ("{} constraint {} violated: {!r} is not satisfied by "
                "{!r}").format(self.relname,
                               self.cname,
                               self.constraint,
                               self.invalid)


class _Rels:

    def __init__(self, db, storage):
        self._db = db
        self._storage = storage
        for attrname, val in storage.relations():
            val._db = self._db
            val.__name__ = attrname
            super().__setattr__(attrname, val)

    def _iter_rels(self):
        return [(n, r) for n, r in self.__dict__.items()
                       if not n.startswith('_')]

    def __setattr__(self, name, val):
        if name.startswith('_'):
            if hasattr(val, 'header'):
                raise ValueError("Relation names may not begin with '_'")
            super().__setattr__(name, val)
            return
        attr = getattr(self, name, None)
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
        self._db._check_constraints(name, val)
        val._db = self._db
        val.__name__ = name
        self._storage.update_relation(name, val)
        super().__setattr__(name, val)


class Database:

    def __init__(self, fn, debug_sql=False):
        storage = self._storage = _dumb_sqlite_persistence(fn, debug_sql=False)
        self._init()
        self.r = _Rels(self, storage)
        self.row_constraints.update(storage.get_row_constraints())

    def _init(self):
        self.row_constraints = _collections.defaultdict(dict)
        self._constraint_ns = _collections.ChainMap(_all)
        self._constraints = {}

    def __repr__(self):
        return "{}({{{}}})".format(
            self.__class__.__name__,
            ', '.join("{!r}: {!r}".format(n, type(r))
                      for n, r in sorted(self.r._iter_rels())))

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
        # Nullify all the relation attributes.  This does two things: makes
        # them inaccessible, and breaks the reference cycle between the
        # Database object and the relations.
        del self.r
        self._init()

    # Row Constraints

    def constrain_rows(self, relname, **kw):
        r = getattr(self.r, relname)
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
            "_row_ >> _key_{}._header.keys() not in _key_{}".format(
                relname, relname))

    def _update_key(self, relname):
        r = getattr(self.r, relname)
        key = self._keys[relname]
        if len(r) < len(key):
            self._keys[relname] = key | (r - key) >> key._header.keys()
        else:
            self._keys[relname] = matching(key, r)
        return True

    def key(self, relname):
        return set(self._constraint_ns['_key_'+relname]._header.keys())



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
            r = rel(**header)()
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




# "import *" support: use this only when playing with relational algebra in the
# Python shell.
__all__ = list(_names) + ['ns', 'expression_namespace', 'Database']


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
