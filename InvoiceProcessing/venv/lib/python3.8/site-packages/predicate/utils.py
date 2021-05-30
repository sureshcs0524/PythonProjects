from copy import deepcopy
from functools import reduce, wraps
from typing import Any, Callable, Iterable, List, Optional, TypeVar

A = TypeVar('A')
B = TypeVar('B')


def flip(func) -> Callable:
    """
    Reverses the Arity of a function
    """
    @wraps(func)
    def flipped(*args):
        return func(*reversed(args))

    return flipped


def identity(a: A) -> A:
    return a


def apply_to(value: A, func: Callable[[A], B]) -> B:
    """
    Inverse of call. Applies an argument to a function
    """
    return func(value)


def compose(*functions: Callable) -> Callable:
    """
    Functional Compose
    """
    return reduce(lambda f, g: lambda x: f(g(x)), functions, identity)


def contains(haystack: Iterable[A], needle: A) -> bool:
    """
    Functional wrapper around `in`
    >>> contains("test", 'e')
    True
    >>> contains([1,2,3], 2)
    True
    >>> contains({'a':1, 'b':2}, 'a')
    True
    >>> contains("Foo", 'bar')
    False
    """
    return needle in haystack


def startswith(target: str, start: str) -> bool:
    """
    Function String.startswith
    >>> startswith("Test", 'Te')
    True
    >>> startswith("Foo", "bar")
    False
    """
    return target.startswith(start)


def endswith(target: str, end: str) -> bool:
    """
    Functional String.endswith
    >>> endswith("Test", 'st')
    True
    >>> endswith("Foo", 'bar')
    False
    """
    return target.endswith(end)


def iexact(a: str, b: str) -> bool:
    """
    Case insensitive comparison
    >>> iexact('Foo', 'foo')
    True
    >>> iexact('Foo', "Bar")
    False
    """
    return a.lower() == b.lower()


def safe_lens(path: List[str]) -> Callable[[Any], Optional[Any]]:
    """
    Digs into nested structures defaulting to None

    >>> safe_lens(['foo'])({'foo': 'bar'})
    'bar'
    >>> safe_lens(['foo', 'bar'])({'foo': {'bar': 'baz'}})
    'baz'
    >>> safe_lens(['foo', 'bar'])({'foo': 'bar'})

    """

    def lens(target):
        _data = deepcopy(target)
        keypath = path[:]
        while keypath and _data is not None:
            k = keypath.pop(0)
            if isinstance(_data, dict):
                _data = _data.get(str(k), None)
            else:
                _data = getattr(_data, str(k), None)
        return _data

    return lens
