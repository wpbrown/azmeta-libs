from typing import NamedTuple, List, Any, Iterable, Union, Sequence
from pandas import DataFrame, Series
from ..utils.types import realize_sequence
import json

class KustoColumnDescriptor(NamedTuple):
    name: str
    type: str


def kusto_data_to_dataframe(columns: Sequence[KustoColumnDescriptor], rows: Iterable[Sequence[Any]]) -> DataFrame:
    rows = realize_sequence(rows)
    series = {name: _make_series((x[index] for x in rows), kdtype) for index, (name, kdtype) in enumerate(columns)}
    return DataFrame(series)


def _make_series(data: Iterable[Any], kusto_datatype: str) -> Series:
    dtype = _kusto_datatype_map[kusto_datatype]
    if kusto_datatype == 'dynamic':
        data = (_parse_dynamic(v) if isinstance(v, str) else v for v in data)

    return Series(data, dtype=dtype)


def _parse_dynamic(value: str) -> Any:
    try:
        return json.loads(value)
    except Exception:
        return value


_kusto_datatype_map = {
    'bool': 'boolean',
    'datetime': 'string',
    'dynamic': 'object',
    'guid': 'string',
    'int': 'Int32',
    'long': 'Int64',
    'real': 'float64',
    'string': 'string',
    'timespan': 'string',
    'decimal': 'float64'
}