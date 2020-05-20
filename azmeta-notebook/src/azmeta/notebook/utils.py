import os
from typing import List, Sequence


def uncommon_substrings(strings: Sequence[str]) -> List[str]:
    prefix = os.path.commonprefix(strings)
    suffix = os.path.commonprefix([i[::-1] for i in strings])[::-1]

    prefix_len = len(prefix)
    suffix_len = len(suffix)
    if prefix_len == 0 and suffix_len == 0:
        return list(strings)

    
    return [s[prefix_len:suffix_len * -1] for s in strings]
