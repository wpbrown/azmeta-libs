from azure.common.client_factory import get_client_from_cli_profile
from azure.mgmt.resourcegraph import ResourceGraphClient
from azure.mgmt.resourcegraph.models import QueryRequest, QueryRequestOptions, QueryResponse, ResultTruncated
from typing import List, Iterable, Tuple, Any
from azure.loganalytics.models import QueryBody, QueryResults
from azure.kusto.data.response import KustoResponseDataSetV1, KustoResponseDataSet
from azure.kusto.data.helpers import dataframe_from_result_table
from .utils.types import realize_sequence
from .kusto import KustoDataFrameResponse, KustoColumnDescriptor, kusto_data_to_dataframe
import pandas
import itertools


def query_kusto(subscriptions: Iterable[str], query: str, max_pages = 10) -> KustoResponseDataSet:
    responses = _query_native(subscriptions, query, max_pages)

    column_data = [{'ColumnName': c['name'], 'ColumnType': c['type']} for c in responses[0].data['columns']]
    row_data = list(itertools.chain(r.data['rows'] for r in responses))
    json_data = { 'Tables': [{ 'TableName': 'PrimaryResult', 'Rows': row_data, 'Columns': column_data}] }

    return KustoResponseDataSetV1(json_data)


def query_dataframe(subscriptions: Iterable[str], query: str, max_pages = 10) -> KustoDataFrameResponse:
    responses = _query_native(subscriptions, query, max_pages)
    
    columns = [KustoColumnDescriptor(c['name'], _RESOURCE_GRAPH_TO_KQL_TYPE_MAP[c['type']]) for c in responses[0].data['columns']]
    rows = itertools.chain.from_iterable(r.data['rows'] for r in responses)

    return KustoDataFrameResponse([kusto_data_to_dataframe(columns, rows)])


_RESOURCE_GRAPH_TO_KQL_TYPE_MAP = {
    'number': 'real',
    'integer': 'long',
    'object': 'dynamic',
    'string': 'string'
}


def _query_native(subscriptions: Iterable[str], query: str, max_pages = 10) -> List[QueryResponse]:
    client = get_client_from_cli_profile(ResourceGraphClient)
    query_options = QueryRequestOptions()
    query_request = QueryRequest(subscriptions=realize_sequence(subscriptions), query=query, options=query_options)
    query_response: QueryResponse = client.resources(query_request)

    if query_response.result_truncated is ResultTruncated.true:
        raise RuntimeError("results are truncated. project id to enable paging.")

    if query_response.skip_token:
        page_size = query_response.count
        if query_response.total_records > page_size * max_pages:
            raise RuntimeError("too many results. increase max pages.")
    
    responses = [query_response]
    while query_response.skip_token:
        query_options = QueryRequestOptions(skip_token=query_response.skip_token)
        query_request = QueryRequest(subscriptions=subscriptions, query=query, options=query_options)
        query_response = client.resources(query_request)
        responses.append(query_response)

    return responses