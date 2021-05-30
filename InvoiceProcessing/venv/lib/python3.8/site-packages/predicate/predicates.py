from functools import partial
from typing import Any, Callable, Dict, List, Optional, Tuple, TypeVar

from .comparator import COMPARATORS, Comparator, comparator_from_string
from .utils import safe_lens, compose, flip

A = TypeVar('A')
B = TypeVar('B')

Predicate = Callable[[A], bool]


def kwargs_to_predicates(key_word_arguments_dict: Dict[str, Any]) -> List[Predicate]:
    """
    Transform the Dictionary of Key Word arguments into a iterable of predicate functions
    """
    return [_process_predicate(key, value) for key, value in key_word_arguments_dict.items()]


def _process_predicate(predicate_field: str, value: Any) -> Predicate:
    """
    Take a field and a value and return a predicate function
    """
    attribute_accessor, comparator = _process_predicate_field(predicate_field)
    return compose(partial(flip(COMPARATORS[comparator]), value), attribute_accessor)


def _process_predicate_field(field: str) -> Tuple[Callable[[Any], Optional[Any]], Comparator]:
    """
    breaks down the first part of the kwargs into a nested attribute accessor and a comparison function defaulting to eq

    >>> getter, comparator = _process_predicate_field('name')
    >>> getter({'name': 'bob'})
    'bob'
    >>> comparator == Comparator.EQ
    True
    >>> getter, comparator = _process_predicate_field('name__first__iexact')
    >>> getter({'name': {'first': 'Joe'}})
    'Joe'
    >>> comparator == Comparator.IEXACT
    True

    """
    parts = field.split("__")
    *init, last = parts
    maybe_comparator = comparator_from_string(last)
    return (safe_lens(init), maybe_comparator) if maybe_comparator is not None else (safe_lens(parts), Comparator.EQ)
