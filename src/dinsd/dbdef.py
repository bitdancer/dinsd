from collections import Mapping
from operator import attrgetter, itemgetter


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
        return "{}._row_({{{}}})".format(self._relation_name_,
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
        dct['_degree_'] = len(attrs)
        dct['_attr_names_'] = sorted(attrs)
        dct['_header_'] = {n: dct[n] for n in attrs}
        class _rowclass_(Row):
            _header_ = dct['_header_']
            _degree_ = len(attrs)
            _relation_name_ = name
        dct['_row_'] = _rowclass_
        return type.__new__(cls, name, bases, dct)
    

class Relation(RichCompareMixin, metaclass=RelationMeta):

    def __init__(self, *args):
        if len(args) == 0:
            self._rows_ = set()
            return
        if hasattr(args[0], 'items'):
            self._rows_ = {self._row_(x) for x in args}
            return
        attrlist = args[0]
        self._validate_attr_list(attrlist)
        rows = set()
        for i, row in enumerate(args[1:], start=1):
            if len(row) != self._degree_:
                raise TypeError(
                    "Expected {} attributes, got {} in row {}".format(
                        self._degree_, len(row), i))
            try:
                rows.add(self._row_({k: v for k, v in zip(attrlist, row)}))
            except TypeError as e:
                raise TypeError(str(e) + " in row {}".format(i))
        self._rows_ = rows

    def _validate_attr_list(self, attrlist):
        if len(attrlist) != self._degree_:
            raise TypeError(
                "Expected {} attributes, got {} in header row".format(
                    self._degree_, len(attrlist)))
        # Make sure the names name our attributes (AttributeError otherwise)
        for attr in attrlist:
            getattr(self, attr)

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
        r += ', '.join([repr(x) for x in self._attr_names_])
        if not self._rows_:
            return r + ')'
        r += '), '
        rows = []
        for row in sorted(self._rows_, key=attrgetter(*self._attr_names_)):
            rows.append('(' + ', '.join([repr(row[x]) 
                                         for x in self._attr_names_]) + ')')
        r += ', '.join(rows) + ')'
        return r

    def _display_(self, *columns, sort=[]):
        toprint = [columns]
        getter = attrgetter(*columns)
        tosort = [sort] if isinstance(sort, str) else sort
        if not tosort:
            tosort = columns
        indexes = []
        for c in tosort:
            indexes.append(columns.index(c))
        sortgetter = itemgetter(*indexes)
        toprint.extend(sorted([[str(x)
                        for x in getter(row)] for row in self._rows_],
                        key=sortgetter))
        widths = [max([len(x) for x in vals]) for vals in zip(*toprint)]
        sep = '+' + '+'.join(['-'*(w+2) for w in widths]) + '+'
        tline = lambda row: ('| ' +
                             ' | '.join(v.ljust(w)
                                        for v, w in zip(row, widths)) + ' |')
        r = [sep, tline(columns), sep]
        r.extend(tline(row) for row in toprint[1:])
        r.append(sep)
        return '\n'.join(r)

    def __str__(self):
        return self._display_(*self._attr_names_)
