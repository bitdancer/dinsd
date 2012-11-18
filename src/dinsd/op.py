from operator import attrgetter
from collections import defaultdict
from itertools import accumulate, repeat
from dinsd.dbdef import Relation, Dum, Dee

# For debugging only.
import sys
dbg = lambda *args: print(*args, file=sys.stderr)

#
# Dynamic relation creation.
#

def rel(*args, **kw):
    name = None
    if len(args)==1:
        kw = dict(args[0], **kw)
    if '__name__' in kw:
        name = kw.pop('__name__')
    if any(n.startswith('_') for n in kw):
        raise ValueError("Invalid relational attribute name {!r}".format(
            [n for n in sorted(kw) if n.startswith('_')][0]))
    if name:
        return type(name, (Relation,), kw)
    return _rel('rel', kw)

def _rel(prefix, attr_dict):
    new_Rel_name = prefix + '_' + '_'.join(sorted(attr_dict.keys()))
    new_Rel = type(new_Rel_name, (Relation,), attr_dict.copy())
    return new_Rel

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

# Make '&' the same as binary join for Relations.
Relation.__and__ = lambda self, other: _binary_join(self, other)


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

# Make >> the project operator, and << the "all_but" operator.
Relation.__rshift__ = lambda self, other: project(self, other)
Relation.__lshift__ = lambda self, other: project(self, all_but(other))


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

# Make | the union operator.
Relation.__or__ = lambda self, other: union(self, other)


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

# Make - the notmatching operator.
Relation.__sub__ = lambda self, other: notmatching(self, other)


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
