from typing import Sequence, Iterable, TypeVar

T = TypeVar('T')

def realize_sequence(value: Iterable[T]) -> Sequence[T]:
    if isinstance(value, Sequence):
        return value
    
    return list(value)
