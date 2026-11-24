"""A deliberately small predicate language with decidable containment."""
from dataclasses import dataclass
from typing import Optional, Union, Tuple

import pyarrow as pa

Scalar = Union[int, str, bool]


@dataclass(frozen=True)
class Interval:
    lower: Optional[Scalar] = None
    upper: Optional[Scalar] = None
    lower_closed: bool = True
    upper_closed: bool = True

    def __post_init__(self):
        values = [v for v in (self.lower, self.upper) if v is not None]
        if not values or any(type(v) not in (int, str, bool) for v in values):
            raise ValueError("Use integer bounds or a non-null string/boolean equality")
        if len({type(v) for v in values}) != 1:
            raise ValueError("Bounds must have identical types")
        if type(values[0]) is not int and not (
            self.lower == self.upper and self.lower_closed and self.upper_closed
        ):
            raise ValueError("Strings and booleans support equality only")
        if self.lower is not None and self.upper is not None:
            if self.lower > self.upper or (self.lower == self.upper and not (
                self.lower_closed and self.upper_closed
            )):
                raise ValueError("Empty or reversed interval")

    @classmethod
    def eq(cls, value: Scalar):
        return cls(value, value)

    def contains_value(self, value):
        # SQL comparisons involving NULL never evaluate TRUE.
        if value is None:
            return False
        return (
            (self.lower is None or value > self.lower or
             (self.lower_closed and value == self.lower)) and
            (self.upper is None or value < self.upper or
             (self.upper_closed and value == self.upper))
        )

    def contains(self, other: "Interval") -> bool:
        own_type = type(self.lower if self.lower is not None else self.upper)
        other_type = type(other.lower if other.lower is not None else other.upper)
        if own_type is not other_type:
            return False
        if self.lower is not None:
            if other.lower is None or other.lower < self.lower:
                return False
            if other.lower == self.lower and other.lower_closed and not self.lower_closed:
                return False
        if self.upper is not None:
            if other.upper is None or other.upper > self.upper:
                return False
            if other.upper == self.upper and other.upper_closed and not self.upper_closed:
                return False
        return True

    def to_dict(self):
        return dict(lower=self.lower, upper=self.upper,
                    lower_closed=self.lower_closed, upper_closed=self.upper_closed)


def _literal(value):
    if type(value) is str:
        return "'" + value.replace("'", "''") + "'"
    if type(value) is bool:
        return "TRUE" if value else "FALSE"
    return str(value)


@dataclass(frozen=True)
class Region:
    terms: Tuple[Tuple[str, Interval], ...] = ()

    def __post_init__(self):
        terms = tuple(sorted(self.terms, key=lambda term: term[0]))
        if any(not isinstance(name, str) or not isinstance(bound, Interval)
               for name, bound in terms):
            raise TypeError("Expected (column name, Interval) terms")
        if len({name for name, _ in terms}) != len(terms):
            raise ValueError("Use one interval per column")
        object.__setattr__(self, "terms", terms)

    @classmethod
    def of(cls, **terms):
        return cls(tuple(terms.items()))

    def covers(self, query: "Region") -> bool:
        query_terms = dict(query.terms)
        return all(name in query_terms and bound.contains(query_terms[name])
                   for name, bound in self.terms)

    def validate(self, schema):
        for name, bound in self.terms:
            if schema.names.count(name) != 1:
                raise ValueError("Unknown or ambiguous column: " + name)
            dtype = schema.field(name).type
            value = bound.lower if bound.lower is not None else bound.upper
            supported = ((type(value) is int and pa.types.is_integer(dtype)) or
                         (type(value) is str and pa.types.is_string(dtype)) or
                         (type(value) is bool and pa.types.is_boolean(dtype)))
            if not supported:
                raise TypeError("Unsupported predicate type for " + name + ": " + str(dtype))
            for endpoint in (bound.lower, bound.upper):
                if endpoint is not None:
                    pa.scalar(endpoint, type=dtype)  # reject overflow, never silently cast

    def matches(self, row):
        return all(bound.contains_value(row[name]) for name, bound in self.terms)

    def sql(self):
        expressions = []
        for name, bound in self.terms:
            column = '"' + name.replace('"', '""') + '"'
            if bound.lower == bound.upper:
                expressions.append(f"{column} = {_literal(bound.lower)}")
            else:
                if bound.lower is not None:
                    op = ">=" if bound.lower_closed else ">"
                    expressions.append(f"{column} {op} {_literal(bound.lower)}")
                if bound.upper is not None:
                    op = "<=" if bound.upper_closed else "<"
                    expressions.append(f"{column} {op} {_literal(bound.upper)}")
        return " AND ".join(expressions) or "TRUE"

    def to_dict(self):
        return {name: bound.to_dict() for name, bound in self.terms}

    @classmethod
    def from_dict(cls, data):
        return cls(tuple((name, Interval(**bound)) for name, bound in data.items()))
