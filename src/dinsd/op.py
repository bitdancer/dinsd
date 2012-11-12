from operator import attrgetter
from collections import defaultdict
from dinsd.dbdef import Relation, Dee

def display(relvar, *columns, **kw):
    relvar._validate_attr_list(columns)
    return relvar._display_(*columns, **kw)

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
                    "with different type (first: {}, second: {} "
                    "found in joined relvars of type {} and {}".format(
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
    # Build the new relation type.
    new_Rel_name = 'join_' + '_'.join(sorted(combined_attrs.keys()))
    new_Rel = type(new_Rel_name, (Relation,), combined_attrs)
    # Create an initially empty new relation of the new type, and then extend
    # it with the joined data.  Because the body is a set we don't have to
    # worry about duplicates.
    new_rel = new_Rel()
    for row in first._rows_:
        key = getter(row)
        for row2 in matches(key):
            attrs = row._as_dict_()
            attrs.update(row2._as_dict_())
            new_rel._rows_.add(new_Rel._row_(attrs))
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
    new_Rel_name = 'renamed_' + '_'.join(sorted(new_attrs.keys()))
    new_Rel = type(new_Rel_name, (Relation,), new_attrs)
    new_rel = new_Rel()
    for row in relation._rows_:
        row_data = row._as_dict_()
        holder = {}
        for old, new in renames.items():
            holder[new] = row_data.pop(old)
        row_data.update(holder)
        new_rel._rows_.add(new_Rel._row_(row_data))
    return new_rel


def project(relation, only=None, all_but=None):
    if only and all_but:
        raise TypeError("Only one of only and all_but allowed")
    if only:
        reduced_attrs = {n: t for n, t in relation._header_.items()
                              if n in only}
        if not len(reduced_attrs) == len(only):
            raise TypeError("Attribute list included invalid attributes")
    elif all_but:
        reduced_attrs = relation._header_.copy()
        for name in all_but:
            del reduced_attrs[name]
    elif only is not None:
        reduced_attrs = {}
    else:
        reduced_attrs = relation._header_.copy()
    reduced_attr_names = reduced_attrs.keys()
    new_Rel_name = 'project_' + '_'.join(sorted(reduced_attrs.keys()))
    new_Rel = type(new_Rel_name, (Relation,), reduced_attrs)
    new_rel = new_Rel()
    for row in relation._rows_:
        new_row_data = {n: v for n, v in row._as_dict_().items()
                             if n in reduced_attr_names}
        new_rel._rows_.add(new_Rel._row_(new_row_data))
    return new_rel

# Make >> the project operator, and << the "all_but" operator.
Relation.__rshift__ = lambda self, other: project(self, other)
Relation.__lshift__ = lambda self, other: project(self, all_but=other)
