from operator import attrgetter, itemgetter
from itertools import zip_longest

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

    def __iter__(self):
        return iter(self._rows_)

    def __len__(self):
        return len(self._rows_)

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

    def __hash__(self):
        return hash(self._rows_)

    def __ne__(self, other):
        return not self == other

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
