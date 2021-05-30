from .combinator import combine
from .predicates import Predicate, kwargs_to_predicates


def where(**kwargs) -> Predicate:
    combinator = kwargs.pop('combinator', 'and')
    predicates = kwargs_to_predicates(kwargs)
    return combine(combinator, predicates)
