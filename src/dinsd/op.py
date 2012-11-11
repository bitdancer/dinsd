from operator import attrgetter
from collections import defaultdict
from dinsd.dbdef import Relation

def display(relvar, *columns):
    relvar._validate_attr_list(columns)
    return relvar._display_(*columns)

def join(first, *relvars):
    joined = first
    for i, rel in enumerate(relvars, start=1):
        try:
            joined = _binary_join(joined, rel)
        except TypeError as e:
            raise TypeError(str(e) + " (error detected while processing "
                            "argument {})".format(i))
    return joined

def _binary_join(first, second):
    combined_attrs = {n: getattr(first, n) for n in first._attr_names_}
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


def intersect(first, *relvars):
    for rel in relvars:
        if first._header_ != rel._header_:
            raise TypeError("Cannot take intersection of unlike relations")
    new_rel = type(first)()
    new_rel._rows_ = first._rows_
    for rel in relvars:
        new_rel._rows_ = new_rel._rows_.intersection(rel._rows_)
    return new_rel


def times(first, *relvars):
    for rel in relvars:
        if first._header_.keys() & rel._header_.keys():
            raise TypeError("Cannot multiply relations that share attributes")
    return join(first, *relvars)
