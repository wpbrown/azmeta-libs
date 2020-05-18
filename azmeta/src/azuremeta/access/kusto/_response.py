from azure.kusto.data.helpers import dataframe_from_result_table
from azure.kusto.data.response import KustoResponseDataSet
from typing import TypeVar, Generic, Sequence, Optional
from pandas import DataFrame


class KustoDataFrameResponse:
    def __init__(self, tables: Sequence[DataFrame], native_response: KustoResponseDataSet = None) -> None:
        self.tables = tables
        self._native_response = native_response

    @property
    def primary_result(self) -> DataFrame:
        return self.tables[0]

    @property
    def primary_results(self) -> Sequence[DataFrame]:
        return self.tables

    @property
    def native_response(self) -> Optional[KustoResponseDataSet]:
        return self._native_response


def dataframe_response_from_kusto_response(response: KustoResponseDataSet) -> KustoDataFrameResponse:
    tables = [dataframe_from_result_table(x) for x in response.primary_results]
    return KustoDataFrameResponse(tables, response)