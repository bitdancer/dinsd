from operator import attrgetter, itemgetter
from collections import defaultdict
from itertools import accumulate, repeat, chain, zip_longest

# For debugging only.
import sys
dbg = lambda *args: print(*args, file=sys.stderr)

class RichCompareMixin:

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


class Scaler(RichCompareMixin):

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


class RowMeta(type):

    def __eq__(self, other):
        if not isinstance(other, RowMeta):
            return NotImplemented
        return self._header_ == other._header_

    def __hash__(self):
        return super().__hash__()


class Row(RichCompareMixin, metaclass=RowMeta):

    def __init__(self, attrdict):
        if isinstance(attrdict, Row):
            # We are being called as a type function.
            if self._header_ != attrdict._header_:
                raise TypeError("Invalid Row type: {!r}".format(attrdict))
            self.__dict__ = attrdict.__dict__
            return
        if len(attrdict) != self._degree_:
            raise TypeError("Expected {} attributes, got {} ({!r})".format(
                                self._degree_, len(attrdict), attrdict))
        for attr, value in attrdict.items():
            try:
                setattr(self, attr, self._header_[attr](value))
            except (TypeError, ValueError) as e:
                raise type(e)(str(e) + "; {} invalid for attribute {}".format(
                                repr(value), attr))
            except KeyError:
                raise TypeError(
                    "Invalid attribute name {}".format(attr)) from None

    def __getitem__(self, key):
        return getattr(self, key)

    def __len__(self):
        return len(self.__dict__)

    def __iter__(self):
        return iter(self.__dict__)

    def _cmpkey(self):
        return tuple(sorted(self.__dict__.items()))

    def _compare(self, other, method):
        if not isinstance(other, Row) or self._header_ != other._header_:
            return NotImplemented
        return super()._compare(other, method)

    def __eq__(self, other):
        if not isinstance(other, Row):
            return False
        return self._cmpkey() == other._cmpkey()

    def __ne__(self, other):
        return not self == other

    def __hash__(self):
        return hash(self._cmpkey())

    def __repr__(self):
        name = '' if self._relation_name is None else self._relation_name+'.'
        return "{}row({{{}}})".format(
            name,
            ', '.join("{!r}: {!r}".format(k, v)
                        for k, v in sorted(self.__dict__.items())))

    def __str__(self):
        return '({})'.format(
            ', '.join('{}={}'.format(k, v)
                        for k, v in sorted(self.__dict__.items())))

    def _as_dict_(self):
        return self.__dict__.copy()

    def _as_locals_(self):
        l = self.__dict__.copy()
        l['r'] = self
        return l


def row(*args, **kw):
    if len(args)==1:
        kw = dict(args[0], **kw)
    if any(n.startswith('_') for n in kw):
        raise ValueError("Invalid relational attribute name {!r}".format(
            [n for n in sorted(kw) if n.startswith('_')][0]))
    dct = {'_header_': {n: type(v) for n, v in kw.items()},
           '_degree_': len(kw),
           '_relation_name': None}
    cls = type('row_' + '_'.join(sorted(kw.keys())), (Row,), dct)
    return cls(kw)


class RelationMeta(type):

    def __new__(cls, name, bases, dct):
        attrs = [x for x in dct if not x.startswith('_')]
        header = {name: dct.pop(name) for name in attrs}
        dct['header'] = header
        dct['degree'] = len(attrs)
        class RowClass(Row):
            _header_ = header
            _degree_ = len(attrs)
            _relation_name = name
        dct['row'] = RowClass
        RowClass.__name__ = '.'.join((name, 'RowClass'))
        return type.__new__(cls, name, bases, dct)

    # XXX: This doesn't work, punt on it for now.
    #@property
    #def header(self):
    #    return self._header

    #@property
    #def degree(self):
    #    return len(self._header)

    def __eq__(self, other):
        if not isinstance(other, RelationMeta):
            return NotImplemented
        return self.header == other.header

    def __hash__(self):
        return super().__hash__()

    def __repr__(self):
        r = "{}({{".format(self.__name__)
        r += ', '.join([repr(n)+': '+(v.__name__)
                        for n, v in sorted(self.header.items())])
        r += '})'
        return r


class Relation(RichCompareMixin, metaclass=RelationMeta):

    def __init__(self, *args):
        # Several cases: (1) empty relation (2) being called as a type
        # validation function (single arg is a Relation) (3) header tuple
        # followed by value tuples (4) iterable of dict-likes and/or Rows,
        # which could be a single argument or could be one dict/Row per arg.
        if len(args) == 0:
            # (1) Empty relation.
            self._rows_ = set()
            return
        if (len(args)==1 and isinstance(args[0], Relation) and
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
                    raise TypeError(
                        "Expected {} attributes, got {} in row {} for {}".format(
                            self.degree, len(o), i, self.__class__.__name__))
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
            raise TypeError("Expected {} attributes({}), got {} ({}) in "
                    "header row for {}".format(
                    self.degree, sorted(self.header),
                    len(attrlist), sorted(attrlist),
                    self.__class__.__name__))
        for attr in attrlist:
            if attr not in self.header:
                raise AttributeError("{!r} relation has no attribute {!r}".format(
                                     self.__class__.__name__, attr))

    # Miscellaneous operators.

    def __iter__(self):
        return iter(self._rows_)

    def __len__(self):
        return len(self._rows_)

    # Comparison operators (see RichCompareMixin).

    def _cmpkey(self):
        return self._rows_

    def _compare(self, other, method):
        if not isinstance(other, Relation) or self.header != other.header:
            return NotImplemented
        return super()._compare(other, method)

    def __eq__(self, other):
        if not isinstance(other, Relation):
            return False
        if self.header != other.header:
            return False
        return self._rows_ == other._rows_

    def __ne__(self, other):
        return not self == other

    def __hash__(self):
        return hash(self._rows_)

    # Infix Relational operators.

    def __and__(self, other):                   # &
        return _binary_join(self, other)

    def __rshift__(self, other):                # >>
        return project(self, other)

    def __lshift__(self, other):                # <<
        return project(self, all_but(other))

    def __or__(self, other):                    # |
        return union(self, other)

    def __sub__(self, other):                   # -
        return notmatching(self, other)

    # Presentation operators.

    def __repr__(self):
        names = sorted(self.header)
        if self._rows_:
            return "{}({{{}}})".format(self.__class__.__name__,
                ', '.join(repr(row)
                          for row in sorted(self._rows_)))
        return repr(self.__class__) + '()'

    def __display__(self, *columns, sort=[]):
        toprint = [list(map(printable, columns))]
        getter = attrgetter(*columns) if columns else lambda x: x
        # Working around a little Python wart here.
        if len(columns) == 1:
            rows = [(printable(getter(row)),) for row in self._rows_]
        else:
            rows = [list(map(printable, getter(row))) for row in self._rows_]
        tosort = [sort] if isinstance(sort, str) else sort
        if not tosort:
            tosort = columns
        indexes = []
        for c in tosort:
            indexes.append(columns.index(c))
        sortgetter = itemgetter(*indexes) if indexes else None
        toprint.extend(sorted(rows, key=sortgetter))
        widths = [max([x.width for x in vals]) for vals in zip(*toprint)]
        sep = '+' + '+'.join(['-'*(w+2) for w in widths]) + '+'
        r = [sep]
        r.extend((_tline(parts, widths) for parts in zip(*toprint[0]))
                 if columns else ['||'])
        r.append(sep)
        if not columns and len(toprint)==2:
            r.append("||")
        else:
            for row in toprint[1:]:
                r.extend(_tline(parts, widths) for parts in zip_longest(*row))
        r.append(sep)
        return '\n'.join(r)

    def __str__(self):
        return self.__display__(*sorted(self.header))


class printable(RichCompareMixin):

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
        return "printable({!r})".format(self.source)


def _tline(parts, widths):
    return ('| ' + ' | '.join(('' if v is None else v).ljust(w)
                                  for v, w in zip(parts, widths)) + ' |')

class DumDee(Relation):
    pass

Dum = DumDee()
Dee = DumDee({})


#
# Dynamic relation creation.
#

def rel(*args, **kw):
    # Three possibilities.  (1) no arguments or keywords, or a single argument
    # that is an empty dictionary or set: a relation literal representing Dum.
    # (2) a single dictionary argument whose values are all types, possibly
    # with additional keyword arguments whose values are types: a relation
    # declaration.  (3) an iterable or a list of arguments consisting of
    # dictionary and/or Row objects all of the same type: a relation literal
    # whose type is determined by the type of the first item in the iterator or
    # the first argument in the argument list.
    name = body = None
    if '__name__' in kw:
        name = kw.pop('__name__')
    if (not args and not kw) or (len(args) == 1 and
            (hasattr(args[0], 'items') or hasattr(args[0], 'union'))
            and not args[0]):
        # (1) Relation literal representing Dum.
        return Dum
    if (not args and kw or len(args) == 1 and hasattr(args[0], 'values') and
            all(isinstance(v, type) for v in args[0].values())):
        # (2) Relation type declaration.
        header = args[0].copy() if args else {}
        header.update(kw)
        if any(n.startswith('_') for n in header):
            raise ValueError("Invalid relational attribute name {!r}".format(
                [n for n in sorted(header) if n.startswith('_')][0]))
    else:
        # (3) Relation literal form.
        if kw:
            raise TypeError("keywords attributes not valid in relation "
                            "literal form of rel call")
        if (len(args) == 1 and not hasattr(args[0], 'items') and
                               not hasattr(args[0], '_header_')):
            # Single argument iterator
            args = args[0]
        iterable = iter(args)
        r = next(iterable)
        if not hasattr(r, '_header_'):
            r = row(r)
        header = r._header_.copy()
        body = chain([r], iterable)
    if name is None:
        name = _make_name('rel', header)
    new_rel = type(name, (Relation,), header)
    return new_rel(body) if body else new_rel

def _make_name(prefix, attrs):
    return prefix + '_' + '_'.join(sorted(attrs))

def _rel(prefix, attr_dict):
    new_Rel_name = _make_name(prefix, attr_dict)
    return type(new_Rel_name, (Relation,), attr_dict.copy())

#
# Relational Operators
#

def join(*relations):
    if not relations:
        return Dee
    if len(relations)==1 and not isinstance(relations[0], Relation):
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
                    "in joined relations with type names {} and {}".format(
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
        getter = attrgetter(*common_attrs)
        index = defaultdict(list)
        for row in second:
            index[getter(row)].append(row)
        matches = lambda key: index[key]
    else:
        getter = lambda row: None
        matches = lambda key: second._rows_
    # Create an initially empty new relation of the new type, and then extend
    # it with the joined data.  Because the body is a set we don't have to
    # worry about duplicates.
    new_rel = _rel('join', combined_attrs)()
    for row in first._rows_:
        key = getter(row)
        for row2 in matches(key):
            attrs = row._as_dict_()
            attrs.update(row2._as_dict_())
            new_rel._rows_.add(new_rel.row(attrs))
    return new_rel


def intersect(*relations):
    if not relations:
        return Dee
    if len(relations)==1 and not isinstance(relations[0], Relation):
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
    if len(relations)==1 and not isinstance(relations[0], Relation):
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
        holder[new] = new_attrs.pop(old)
    new_attrs.update(holder)
    new_rel = _rel('renamed', new_attrs)()
    for row in relation._rows_:
        row_data = row._as_dict_()
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
            raise TypeError("Attribute list included invalid attributes: "
                            "{}".format(self.names - all_names))
        return all_names - self.names


def project(relation, attr_names):
    if hasattr(attr_names, 'all_but'):
        attr_names = attr_names.all_but(relation)
    reduced_attrs = {n: t for n, t in relation.header.items()
                          if n in attr_names}
    if not len(reduced_attrs) == len(attr_names):
        raise TypeError("Attribute list included invalid attributes: "
                        "{}".format(attr_names - reduced_attrs.keys()))
    reduced_attr_names = reduced_attrs.keys()
    new_rel = _rel('project', reduced_attrs)()
    for row in relation._rows_:
        new_row_data = {n: v for n, v in row._as_dict_().items()
                             if n in reduced_attr_names}
        new_rel._rows_.add(new_rel.row(new_row_data))
    return new_rel


def where(relation, selector):
    if isinstance(selector, str):
        selector = lambda r, s=selector: eval(s, r._as_locals_(), _expn)
    new_rel = type(relation)()
    for row in relation._rows_:
        if selector(row):
            new_rel._rows_.add(row)
    return new_rel


def extend(relation, **new_attrs):
    if len(relation) == 0:
        # Tutorial D can do this, but the fact that we can't probably doesn't
        # matter much in practice.
        raise TypeError("Cannot extend empty relation")
    for n, f in new_attrs.items():
        if isinstance(f, str):
            new_attrs[n] = lambda r, s=f: eval(s, r._as_locals_(), _expn)
    attrs = relation.header.copy()
    row1 = next(iter(relation))
    attrs.update({n: type(new_attrs[n](row1)) for n in new_attrs.keys()})
    new_rel = _rel('extend', attrs)()
    for row in relation:
        new_values = row._as_dict_()
        new_values.update({n: new_attrs[n](row) for n in new_attrs.keys()})
        new_rel._rows_.add(new_rel.row(new_values))
    return new_rel


def union(*relations):
    if len(relations) == 0:
        return Dum
    if len(relations)==1 and not isinstance(relations[0], Relation):
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
                    "found in match relation (relation type names "
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
    getter = attrgetter(*common_attrs)
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
# Aggregate Operators
#


def display(relvar, *columns, **kw):
    relvar._validate_attr_list(columns)
    return relvar.__display__(*columns, **kw)


def compute(relvar, func):
    if isinstance(func, str):
        func = lambda r, s=func: eval(s, r._as_locals_(), _expn)
    for row in relvar:
        yield func(row)


def avg(iterator):
    """Calculate the average of an iterator.

    This is a functional, iterative implementation, which means the iterator
    does not have to support len, and we accumulate the result as we go along
    rather than first exhausting the iterator and then performing the
    summation.
    """
    c = 0
    for s, c in accumulate(zip(iterator, repeat(1)),
                           lambda x, y: (x[0]+y[0], x[1]+y[1])):
        pass
    return 0 if c==0 else s/c


#
# Extended Operators
#

def summarize(relvar, compvar, _debug=False, **new_attrs):
    if not isinstance(compvar, Relation):
        # Assume it is an attribute name list
        compvar = relvar >> compvar
    x = extend(compvar, t_e_m_p=lambda r: compose(relvar, type(compvar)(r)))
    if _debug:
        print(x)
    for n, f in new_attrs.items():
        if isinstance(f, str):
            new_attrs[n] = lambda r, s=f: eval(s, {'summary': r.t_e_m_p}, _expn)
        else:
            new_attrs[n] = lambda r, f=f: f(r.t_e_m_p)
    return extend(x, **new_attrs) << {'t_e_m_p'}


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
    new_rel = _rel('ungroup', attrs)()
    for row in relation:
        new_values = row._as_dict_()
        subrel = new_values.pop(attrname)
        for subrow in subrel:
            new_values.update(subrow._as_dict_())
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
    new_rel = _rel('unwrap', attrs)()
    for row in relation:
        new_values = row._as_dict_()
        subrow = new_values.pop(attrname)
        new_values.update(subrow._as_dict_())
        new_rel._rows_.add(new_rel.row(new_values))
    return new_rel

#
# XXX: Temporary expression global namespace.
#
expression_namespace = {n: v for n, v in globals().items()
                             if not n.startswith('_')}
_expn = expression_namespace
