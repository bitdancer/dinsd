from collections import Mapping
from operator import attrgetter, itemgetter
from itertools import zip_longest


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


class Row(RichCompareMixin, Mapping):

    # This could just be a dict subclass except that we want to take
    # advantage of the PEP 412 key sharing instance dictionaries.

    def __init__(self, attrdict):
        if len(attrdict) != self._degree_:
            raise TypeError("Expected {} attributes, got {}".format(
                                self._degree_, len(attrdict)))
        for attr, value in attrdict.items():
            try:
                setattr(self, attr, self._header_[attr](value))
            except (TypeError, ValueError) as e:
                raise type(e)(str(e) + ", {} invalid for attribute {}".format(
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
        if not isinstance(other, Row):
            return False
        return super()._compare(other, method)

    def __repr__(self):
        return "{}.row({{{}}})".format(self._relation_name_,
            ', '.join("{!r}: {!r}".format(k, v)
                        for k, v in sorted(self.__dict__.items())))

    def __str__(self):
        return '({})'.format(
            ', '.join('{}={}'.format(k, v)
                        for k, v in sorted(self.__dict__.items())))

    def _as_dict_(self):
        return self.__dict__.copy()


class RelationMeta(type):

    def __new__(cls, name, bases, dct):
        attrs = [x for x in dct if not x.startswith('_')]
        header = {name: dct.pop(name) for name in attrs}
        dct['header'] = header
        dct['degree'] = len(attrs)
        class RowClass(Row):
            _header_ = header
            _degree_ = len(attrs)
            _relation_name_ = name
        dct['row'] = RowClass
        RowClass.__name__ = '.'.join((name, 'RowClass'))
        return type.__new__(cls, name, bases, dct)


class Relation(RichCompareMixin, metaclass=RelationMeta):

    def __init__(self, *args):
        if len(args) == 0:
            self._rows_ = set()
            return
        if (len(args)==1 and isinstance(args[0], Relation) and
                args[0].header == self.header):
            # We were called as a type validation function.  Return an
            # immutable copy, because the only time this happens is when a
            # relation is the value of an attribute, and when a relation is a
            # value of an attribute it must be immutable.
            self._rows_ = frozenset(args[0]._rows_)
            return
        if hasattr(args[0], 'items'):
            self._rows_ = {self.row(x) for x in args}
            return
        attrlist = args[0]
        self._validate_attr_list(attrlist)
        rows = set()
        for i, row in enumerate(args[1:], start=1):
            if len(row) != self.degree:
                raise TypeError(
                    "Expected {} attributes, got {} in row {}".format(
                        self.degree, len(row), i))
            try:
                rows.add(self.row({k: v for k, v in zip(attrlist, row)}))
            except TypeError as e:
                raise TypeError(str(e) + " in row {}".format(i))
        self._rows_ = rows

    def _validate_attr_list(self, attrlist):
        if len(attrlist) != self.degree:
            raise TypeError(
                "Expected {} attributes, got {} in header row".format(
                    self.degree, len(attrlist)))
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
        if not isinstance(other, Relation):
            return False
        return super()._compare(other, method)

    def __repr__(self):
        r = "{}((".format(self.__class__.__name__)
        names = sorted(self.header)
        r += ', '.join([repr(x) for x in names])
        if not self._rows_:
            return r + '))'
        r += '), '
        rows = []
        for row in sorted(self._rows_, key=attrgetter(*names)):
            rows.append('(' + ', '.join([repr(row[x])
                                         for x in names]) + ')')
        r += ', '.join(rows) + ')'
        return r

    def _display_(self, *columns, sort=[]):
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
        return self._display_(*sorted(self.header))


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
