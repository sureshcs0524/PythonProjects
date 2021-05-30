import operator
from enum import Enum, unique
from typing import Optional

from .utils import apply_to, contains, endswith, flip, iexact, startswith


@unique
class Comparator(Enum):
    EQ = 'eq'
    GT = 'gt'
    GTE = 'gte'
    LT = 'lt'
    LTE = 'lte'
    NE = 'ne'
    FUNC = 'func'
    CONTAINS = 'contains'
    STARTSWITH = 'startswith'
    ENDSWITH = 'endswith'
    EXACT = 'exact'
    IEXACT = 'iexact'
    IN = 'in'
    TYPE = 'type'


def comparator_from_string(string: str) -> Optional[Comparator]:
    try:
        return Comparator(string)
    except ValueError:
        return None


COMPARATORS = {
    Comparator.EQ: operator.eq,
    Comparator.GT: operator.gt,
    Comparator.GTE: operator.ge,
    Comparator.LT: operator.lt,
    Comparator.LTE: operator.le,
    Comparator.NE: operator.ne,
    Comparator.FUNC: apply_to,
    Comparator.CONTAINS: contains,
    Comparator.STARTSWITH: startswith,
    Comparator.ENDSWITH: endswith,
    Comparator.EXACT: operator.eq,
    Comparator.IEXACT: iexact,
    Comparator.IN: flip(contains),
    Comparator.TYPE: isinstance,
}
