from operator import attrgetter
from collections import defaultdict
from dinsd.dbdef import Relation, Dum, Dee

#
# Relational Operators
#

def join(*relvars):
    if not relvars:
        return Dee
    joined, *relvars = relvars
    for i, rel in enumerate(relvars, start=1):
        try:
            joined = _binary_join(joined, rel)
        except TypeError as e:
            raise TypeError(str(e) + " (error detected while processing "
                            "argument {})".format(i))
    return joined


def _binary_join(first, second):
    combined_attrs = first._header_.copy()
    common_attrs = []
    for attr in second._attr_names_:
        if attr in combined_attrs:
            if getattr(second, attr) != combined_attrs[attr]:
                raise TypeError("Duplicate attribute name ({!r}) "
                    "with different type (first: {}, second: {} found "
                    "in joined relvars with type names {} and {}".format(
                        attr,
                        combined_attrs[attr],
                        getattr(second, attr),
                        type(first),
                        type(second),
                        ))
            common_attrs.append(attr)
        else:
            combined_attrs[attr] = getattr(second, attr)
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
    new_rel = _Rel('join', combined_attrs)()
    for row in first._rows_:
        key = getter(row)
        for row2 in matches(key):
            attrs = row._as_dict_()
            attrs.update(row2._as_dict_())
            new_rel._rows_.add(new_rel._row_(attrs))
    return new_rel

# Make '&' the same as binary join for Relations.
Relation.__and__ = lambda self, other: _binary_join(self, other)


def intersect(*relvars):
    if not relvars:
        return Dee
    first, *relvars = relvars
    for rel in relvars:
        if first._header_ != rel._header_:
            raise TypeError("Cannot take intersection of unlike relations")
    new_rel = type(first)()
    new_rel._rows_ = first._rows_
    for rel in relvars:
        new_rel._rows_ = new_rel._rows_.intersection(rel._rows_)
    return new_rel


def times(*relvars):
    if not relvars:
        return Dee
    first, *relvars = relvars
    for rel in relvars:
        if first._header_.keys() & rel._header_.keys():
            raise TypeError("Cannot multiply relations that share attributes")
    return join(first, *relvars)


def rename(relation, **renames):
    new_attrs = relation._header_.copy()
    holder = {}
    for old, new in renames.items():
        holder[new] = new_attrs.pop(old)
    new_attrs.update(holder)
    new_rel = _Rel('renamed', new_attrs)()
    for row in relation._rows_:
        row_data = row._as_dict_()
        holder = {}
        for old, new in renames.items():
            holder[new] = row_data.pop(old)
        row_data.update(holder)
        new_rel._rows_.add(new_rel._row_(row_data))
    return new_rel


def project(relation, only=None, all_but=None):
    if only and all_but:
        raise TypeError("Only one of only and all_but allowed")
    if only:
        reduced_attrs = {n: t for n, t in relation._header_.items()
                              if n in only}
        if not len(reduced_attrs) == len(only):
            raise TypeError("Attribute list included invalid attributes: "
                            "{}".format(only - reduced_attrs.keys()))
    elif all_but:
        reduced_attrs = relation._header_.copy()
        for name in all_but:
            del reduced_attrs[name]
    elif only is not None:
        reduced_attrs = {}
    else:
        reduced_attrs = relation._header_.copy()
    reduced_attr_names = reduced_attrs.keys()
    new_rel = _Rel('project', reduced_attrs)()
    for row in relation._rows_:
        new_row_data = {n: v for n, v in row._as_dict_().items()
                             if n in reduced_attr_names}
        new_rel._rows_.add(new_rel._row_(new_row_data))
    return new_rel

# Make >> the project operator, and << the "all_but" operator.
Relation.__rshift__ = lambda self, other: project(self, other)
Relation.__lshift__ = lambda self, other: project(self, all_but=other)


def Rel(**kw):
    return _Rel('rel', kw)

def _Rel(prefix, attr_dict):
    new_Rel_name = prefix + '_' + '_'.join(sorted(attr_dict.keys()))
    new_Rel = type(new_Rel_name, (Relation,), attr_dict)
    return new_Rel


def where(relation, selector):
    new_rel = type(relation)()
    for row in relation._rows_:
        if selector(row):
            new_rel._rows_.add(row)
    return new_rel


def extend(relation, **new_attrs):
    if len(relation) == 0:
        # XXX: this violates TTM, but probably doesn't matter much in practice.
        raise TypeError("Cannot extend empty relation")
    attrs = relation._header_.copy()
    row1 = next(iter(relation))
    attrs.update({n: type(new_attrs[n](row1)) for n in new_attrs.keys()})
    new_rel = _Rel('extend', attrs)()
    for row in relation:
        new_values = row._as_dict_()
        new_values.update({n: new_attrs[n](row) for n in new_attrs.keys()})
        new_rel._rows_.add(new_rel._row_(new_values))
    return new_rel


def union(*relvars):
    if len(relvars) == 0:
        return Dum
    first, *relvars = relvars
    if not all(first._header_ == r._header_ for r in relvars):
        raise TypeError("Union operands must of equal types")
    if isinstance(first, type) and not relvars:
        return first()
    new_rel = type(first)()
    new_rel._rows_.update(first._rows_.copy())
    for rel in relvars:
        new_rel._rows_.update(rel._rows_.copy())
    return new_rel

# Make | the union operator.
Relation.__or__ = lambda self, other: union(self, other)


# XXX: this should probably be a public API of some sort.
def _common_attrs(first, second):
    common_attrs = set()
    for attr in second._attr_names_:
        if attr in first._attr_names_:
            if getattr(first, attr) != getattr(second, attr):
                raise TypeError("Duplicate attribute name ({!r}) "
                    "with different type (first: {}, second: {} "
                    "found in match relation (relation type names "
                    "are {} and {})".format(
                        attr,
                        getattr(first, attr),
                        getattr(second, attr),
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

# Make - the notmatching operator.
Relation.__sub__ = lambda self, other: notmatching(self, other)


def minus(first, second):
    if not first._header_ == second._header_:
        raise TypeError("Relation types must match for minus operation")
    return notmatching(first, second)


def matching(first, second):
    return _matcher(first, second, match=True)


def compose(first, second):
    common_attrs = _common_attrs(first, second)
    return project(join(first, second), all_but=common_attrs)


#
# Aggregate Operators
#


def display(relvar, *columns, **kw):
    return relvar._display_(*columns, **kw)


def compute(relvar, func):
    for row in relvar:
        yield func(row)

def column(relvar, *colnames):
    return compute(relvar, attrgetter(*colnames))
