from collections import Mapping
from operator import itemgetter


class Scaler:

    def __str__(self):
        return str(self.value)

    def __repr__(self):
        return "{}({!r})".format(self.__class__.__name__, self.value)

    def _cmpkey(self):
        return self.value

    def _compare(self, other, method):
        try:
            return method(self._cmpkey(), other._cmpkey())
        except (AttributeError, TypeError):
            # _cmpkey not implemented, or return different type,
            # so I can't compare with "other".
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


class Row(Mapping):

    def __init__(self, attrdict):
        if len(attrdict) != self._degree_:
            raise TypeError("Expected {} attributes, got {}".format(
                                self._degree_, len(attrdict)))
        for attr, value in attrdict.items():
            try:
                setattr(self, attr, self._header[attr](value))
            except KeyError:
                raise TypeError(
                    "Invalid attribute name {}".format(attr)) from None

    def __getitem__(self, key):
        return getattr(self, key)

    def __len__(self):
        return len(self.__dict__)

    def __iter__(self):
        return iter(self.__dict__)

    def __repr__(self):
        return "{}._row_({{{}}})".format(self._relation_name_,
            ', '.join("{!r}: {!r}".format(k, v)
                        for k, v in sorted(self.__dict__.items())))

    def __str__(self):
        return '({})'.format(
            ', '.join('{}={}'.format(k, v)
                        for k, v in sorted(self.__dict__.items())))


class RelationMeta(type):

    def __new__(cls, name, bases, dct):
        attrs = [x for x in dct if not x.startswith('_')]
        dct['_degree_'] = len(attrs)
        dct['_sorted_attrs_'] = sorted(attrs)
        class _rowclass_(Row):
            _header = dct           # XXX: is there a better way to do this?
            _degree_ = len(attrs)
            _relation_name_ = name
        dct['_row_'] = _rowclass_
        return type.__new__(cls, name, bases, dct)
    

class Relation(metaclass=RelationMeta):

    def __init__(self, *args):
        if len(args) == 0:
            self.rows = []
            return
        if hasattr(args[0], 'items'):
            self.rows = [self._row_(x) for x in args]
            return
        attrlist = args[0]
        if len(attrlist) != self._degree_:
            raise TypeError("Expected {} attributes, got {}".format(
                                self._degree_, len(attrlist)))
        rows = []
        for row in args[1:]:
            rows.append(self._row_({k: v for k, v in zip(attrlist, row)}))
        self.rows = rows

    #@classmethod
    #def _row_(self, d):
    #    if len(d) != self._degree_:
    #        raise TypeError("Expected {} attributes, got {}".format(
    #                            self._degree_, len(d)))
    #    return {k: getattr(self, k)(v) for k, v in d.items()}

    def __eq__(self, other):
        if type(self) != type(other):
            return False
        return all(x == y for x, y in zip(self.rows, other.rows))


    def __repr__(self):
        r = "{}((".format(self.__class__.__name__)
        r += ', '.join([repr(x) for x in self._sorted_attrs_])
        if not self.rows:
            return r + ')'
        r += '), '
        rows = []
        for row in sorted(self.rows, key=itemgetter(*self._sorted_attrs_)):
            rows.append('(' + ', '.join([repr(row[x]) 
                                         for x in self._sorted_attrs_]) + ')')
        r += ', '.join(rows) + ')'
        return r

    def __str__(self):
        toprint = [self._sorted_attrs_]
        getter = itemgetter(*self._sorted_attrs_)
        toprint.extend(sorted([str(x)
                        for x in getter(row)] for row in self.rows))
        widths = [max([len(x) for x in vals]) for vals in zip(*toprint)]
        sep = '+' + '+'.join(['-'*(w+2) for w in widths]) + '+'
        tline = lambda row: ('| ' +
                             ' | '.join(v.ljust(w)
                                        for v, w in zip(row, widths)) + ' |')
        r = [sep, tline(self._sorted_attrs_), sep]
        r.extend(tline(row) for row in toprint[1:])
        r.append(sep)
        return '\n'.join(r)
