from datetime import date, datetime
import json
from pandas import DataFrame
from typing import Any

def serialize_to_kql(value: Any) -> str:
    if value is None:
        return "dynamic(null)"
    
    if isinstance(value, str):
        return repr(value)
    
    if isinstance(value, list) or isinstance(value, tuple):
        return f"dynamic({json.dumps(list(value))})"

    if isinstance(value, datetime) or isinstance(value, date):
        return f"datetime({value.isoformat()})"

    if isinstance(value, DataFrame):
        return _serialize_dataframe_to_kql(value)
    
    raise ValueError("can't convert to kql")


# DataFrames

def _serialize_dataframe_to_kql(value: DataFrame) -> str:
    pass


# def _datatable(self, df: DataFrame) -> str:
#     t = {col: str(t).split(".")[-1].split("[",1)[0] for col, t in dict(df.dtypes).items()}
#     d = df.to_dict("split")
#     c = d["columns"]
#     r = d["data"]
#     pairs_t = {col: [str(t[col]), _DATAFRAME_TO_KQL_TYPES.get(str(t[col]))] for col in c}
#     schema = ", ".join([f"{col}:{pairs_t[col][1]}" for col in c])
#     data = ", ".join([", ".join([_dataframe_to_kql_value(val, pairs_t[c[idx]]) for idx, val in enumerate(row)]) for row in r])
#     return f"datatable ({schema}) [{data}]"


# _DATAFRAME_TO_KQL_TYPES = {
#     "int8": "long",
#     "int16": "long",
#     "int32": "long",
#     "int64": "long",
#     "uint8": "long",
#     "uint16": "long",
#     "uint32": "long",
#     "uint64": "long",
#     "float16": "real",
#     "float32": "real",
#     "float64": "real",
#     "complex64": "dynamic",
#     "complex128": "dynamic",
#     "character": "string",
#     "bytes": "string",
#     "str": "string",
#     "void": "string",
#     "record": "dynamic",
#     "bool": "bool",
#     "datetime": "datetime",
#     "datetime64": "datetime",
#     "object": None,
#     "category": "string",
#     "timedelta": "timespan",
#     "timedelta64": "timespan",
# }


# def _dataframe_to_kql_value(val, pair_type:list) -> str:
#     pd_type, kql_type = pair_type
#     s = str(val)
#     if kql_type == "string":
#         if pd_type == "bytes":
#             s = val.decode("utf-8")
#         return "" if s is None else f"'{s}'"
#     if kql_type == "long": 
#         return 'long(null)' if s == 'nan' else f"long({s})"
#     if kql_type == "real": 
#         return 'real(null)' if s == 'nan' else f"real({s})"
#     if kql_type == "bool": 
#         return 'true' if val == True else 'false' if  val == False else 'bool(null)'
#     if kql_type == "datetime":
#         return 'datetime(null)' if s == "NaT" else f"datetime({s})" # assume utc
#     if kql_type == "timespan":
#         raise ValueError("can't convert timespan")
#     if kql_type == "dynamic":
#         if pd_type == "record":
#             return serialize_to_kql(list(val))
#         elif pd_type in ["complex64", "complex128"]:
#             return serialize_to_kql([val.real, val.imag])
#         else:
#             return serialize_to_kql(val)
        
#     return "" if s is None else repr(s)
