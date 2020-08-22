from azmeta.access.utils.sdk import default_sdk_client
from azure.mgmt.resourcegraph import ResourceGraphClient
from azure.mgmt.resourcegraph.models import QueryRequest, QueryRequestOptions, QueryResponse, ResultTruncated
from typing import List, Iterable, Tuple, Any
from pandas import DataFrame
import itertools

from .utils.types import realize_sequence
from .kusto import KustoColumnDescriptor, kusto_data_to_dataframe


def query_native(subscriptions: Iterable[str], query: str, max_pages = 10) -> List[QueryResponse]:
    return _query_native(subscriptions, query, max_pages)


def query_dataframe(subscriptions: Iterable[str], query: str, max_pages = 10) -> DataFrame:
    responses = _query_native(subscriptions, query, max_pages)
    
    columns = [KustoColumnDescriptor(c['name'], _RESOURCE_GRAPH_TO_KUSTO_TYPE_MAP[c['type']]) for c in responses[0].data['columns']]
    rows = itertools.chain.from_iterable(r.data['rows'] for r in responses)

    return kusto_data_to_dataframe(columns, rows)


_RESOURCE_GRAPH_TO_KUSTO_TYPE_MAP = {
    'number': 'real',
    'integer': 'long',
    'object': 'dynamic',
    'string': 'string'
}


def _query_native(subscriptions: Iterable[str], query: str, max_pages) -> List[QueryResponse]:
    client = default_sdk_client(ResourceGraphClient)
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