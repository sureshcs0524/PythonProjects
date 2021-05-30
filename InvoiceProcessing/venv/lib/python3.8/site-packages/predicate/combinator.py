import operator
from enum import Enum
from functools import partial
from typing import Any, Callable, Dict, List, Union

from .predicates import Predicate
from .utils import apply_to, compose


class Combinator(Enum):
    AND = 'and'
    OR = 'or'
    NONE = 'none'
    NAND = 'nand'


def combinator_from_string(string: str) -> Combinator:
    try:
        return Combinator(string)
    except ValueError:
        raise ValueError(f'{string} is not a valid argument for combinator.')


COMBINATORS: Dict[Combinator, Callable] = {
    Combinator.AND: all,
    Combinator.OR: any,
    Combinator.NONE: compose(operator.not_, any),
    Combinator.NAND: compose(operator.not_, all),
}


def combine(combinator: Union[str, Callable], predicates: List[Predicate]) -> Predicate:
    combinator_func: Predicate = combinator if callable(combinator) else COMBINATORS[combinator_from_string(combinator)]

    def _inner(arg: Any) -> bool:
        return combinator_func(map(partial(apply_to, arg), predicates))

    return _inner
