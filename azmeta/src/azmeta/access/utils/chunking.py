import itertools
from typing import NamedTuple, List, Any, Iterable, Union

class ChunkGroup(NamedTuple):
    id: str
    chunks: List[List[Any]]

    def __len__(self):
        return len(self.chunks)


class GroupedChunkList(NamedTuple):
    total_len: int
    groups: List[ChunkGroup]

    def __len__(self):
        return self.total_len


def build_grouped_chunk_list(input_data, select_value, select_group_key, chunk_size=8) -> GroupedChunkList:
    groups = []
    total_chunks = 0
    for key, datas in itertools.groupby(sorted(input_data, key=select_group_key), key=select_group_key):
        values = (select_value(x) for x in datas)
        chunk_data = list(_chunked_iterable(values, chunk_size))
        total_chunks += len(chunk_data)
        groups.append(ChunkGroup(key, chunk_data))

    return GroupedChunkList(total_chunks, groups)


def _chunked_iterable(iterable, size):
    it = iter(iterable)
    while True:
        chunk = tuple(itertools.islice(it, size))
        if not chunk:
            break
        yield chunk
        

def _idivceil(n, d):
    return -(n // -d)