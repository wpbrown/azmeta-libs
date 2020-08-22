from typing import NamedTuple, List, Any, Iterable, Union, Optional, Callable
from pandas import DataFrame
from azmeta.access.utils.sdk import default_sdk_client
from azure.loganalytics import LogAnalyticsDataClient
from azure.loganalytics.models import QueryBody
from msrest.pipeline import ClientRawResponse
from msrest.exceptions import ClientRequestError
from requests.exceptions import Timeout
from azure.kusto.data.response import KustoResponseDataSetV1, KustoResponseDataSet, WellKnownDataSet
import textwrap 
import itertools
import json
import functools
import logging
import time
from .kusto import serialize_to_kql
from .kusto import KustoDataFrameResponse, KustoColumnDescriptor, kusto_data_to_dataframe
from .utils.chunking import GroupedChunkList
from .utils.types import realize_sequence


class PerformanceCounterSpec(NamedTuple):
    object_: str
    counter: str
    instance: Optional[str] = None
    value_transform: Optional[str] = None


def build_perf_counter_percentile_query(resource_ids: List[str], spec: PerformanceCounterSpec) -> str:
    where_clause = f"where ObjectName == '{spec.object_}' and CounterName == '{spec.counter}'"
    if spec.instance:
        where_clause += f" and InstanceName == '{spec.instance}'"
    transform_pipe = ""
    if spec.value_transform:
        transform_pipe = f"\n        | extend value = {spec.value_transform}"
    query = textwrap.dedent(f"""
        let vm_ids = {serialize_to_kql(resource_ids)}; 
        Perf 
        | where _ResourceId in (vm_ids)
        | {where_clause}
        | project TimeGenerated, _ResourceId, value=CounterValue
        | summarize value=avg(value) by _ResourceId, bin(TimeGenerated, 1m) {transform_pipe}
        | summarize percentiles(value, 50, 80, 90, 95, 99), max(value), samples=count() by _ResourceId
        | project resource_id = _ResourceId, percentile_50th = percentile_value_50, percentile_80th = percentile_value_80, percentile_90th = percentile_value_90, percentile_95th = percentile_value_95, percentile_99th = percentile_value_99, max = max_value, samples
        """)
    return query


def build_disk_percentile_query(resource_ids: List[str]) -> str:
    query = textwrap.dedent(f"""
        let vm_ids = {serialize_to_kql(resource_ids)}; 
        Perf 
        | where _ResourceId in (vm_ids)
        | where ObjectName == 'LogicalDisk' and CounterName in ('Disk Transfers/sec', 'Disk Bytes/sec') and InstanceName contains ':' 
        | project TimeGenerated, _ResourceId, CounterName, InstanceName, value=CounterValue
        | summarize value=avg(value) by _ResourceId, CounterName, InstanceName, bin(TimeGenerated, 1m) 
        | summarize percentiles(value, 50, 80, 90, 95, 99), max(value), samples=count() by _ResourceId, CounterName, InstanceName
        | where samples > 1000
        | project resource_id = _ResourceId, counter_name = CounterName, instance_name = InstanceName, percentile_50th = percentile_value_50, percentile_80th = percentile_value_80, percentile_90th = percentile_value_90, percentile_95th = percentile_value_95, percentile_99th = percentile_value_99, max = max_value, samples
        """)
    return query


def query_kusto(query: str, workspaces: Union[Iterable[str], str], timespan: str = None) -> KustoResponseDataSet:
    query_response = _query_native(query, workspaces, timespan)
    data = _parse_raw_response_to_data_dict(query_response, hide_primary_data=False)
    return _create_kusto_result(data)


def query_dataframe(query: str, workspaces: Union[Iterable[str], str], timespan: str = None) -> KustoDataFrameResponse:
    query_response = _query_native(query, workspaces, timespan)
    data = _parse_raw_response_to_data_dict(query_response, hide_primary_data=True)
    return _create_dataframe_result(data, _create_kusto_result(data))


def query_kusto_by_workspace_chunk(chunked_ids: GroupedChunkList, query_builder, timespan: str = None, logger = None) -> KustoResponseDataSet:
    data_dicts = _query_native_by_workspace_chunk(chunked_ids, query_builder, timespan, logger)
    data = _merge_data_dicts(data_dicts, unhide_primary_data=True)
    return _create_kusto_result(data)


def query_dataframe_by_workspace_chunk(chunked_ids: GroupedChunkList, query_builder, timespan: str = None, logger = None) -> KustoDataFrameResponse:
    data_dicts = _query_native_by_workspace_chunk(chunked_ids, query_builder, timespan, logger)
    data = _merge_data_dicts(data_dicts)
    return _create_dataframe_result(data, _create_kusto_result(data_dicts[0]))


def _load_as_kusto_format(dct: dict, hide_primary_data: bool):
    def mapped(map: dict):
        return {map[k] if k in map else k: v for k,v in dct.items()}

    if 'name' in dct and 'type' in dct:
        return mapped(_load_as_kusto_format_col_map)

    if 'name' in dct and 'rows' in dct:
        new_dct = mapped(_load_as_kusto_format_table_map)
        if hide_primary_data:
            new_dct['_Rows_'] = new_dct['Rows']
            new_dct['Rows'] = []
        return new_dct

    if 'tables' in dct:
        return mapped(_load_as_kusto_format_response_map)
    
    return dct


_load_as_kusto_format_col_map = { 'name': 'ColumnName', 'type': 'ColumnType' }
_load_as_kusto_format_table_map = { 'name': 'TableName', 'rows': 'Rows', 'columns': 'Columns' }
_load_as_kusto_format_response_map = { 'tables': 'Tables' }


def _parse_raw_response_to_data_dict(raw_response: ClientRawResponse, hide_primary_data: bool) -> dict:
    load_json: Callable[[bytes], dict] = lambda content: json.loads(content, object_hook=functools.partial(_load_as_kusto_format, hide_primary_data = hide_primary_data))
    return load_json(raw_response.response.content)


def _merge_data_dicts(datas: List[dict], unhide_primary_data: bool = False) -> dict:
    merged_data = datas[0]
    if unhide_primary_data:
        merged_data['Tables'][0]['Rows'] = merged_data['Tables'][0]['_Rows_']
        target_row_list_name = 'Rows'
    else:
        target_row_list_name = '_Rows_'
    for chunk_data in datas[1:]:
        chunk_rows = chunk_data['Tables'][0]['_Rows_']
        merged_data['Tables'][0][target_row_list_name].extend(chunk_rows)

    return merged_data


def _create_kusto_result(data: dict) -> KustoResponseDataSet:
    return KustoResponseDataSetV1(data)


def _create_dataframe_result(data: dict, kusto_response: KustoResponseDataSet) -> KustoDataFrameResponse:
    dataframes = []
    for table in kusto_response.tables:
        if table.table_kind == WellKnownDataSet.PrimaryResult:
            table_data = data['Tables'][table.table_id]
            columns = [KustoColumnDescriptor(c['ColumnName'], c['ColumnType']) for c in table_data['Columns']]
            rows = table_data['_Rows_']
            dataframe = kusto_data_to_dataframe(columns, rows) 
            dataframes.append(dataframe)

    return KustoDataFrameResponse(dataframes, kusto_response)


def _query_native(query: str, workspaces: Union[Iterable[str], str], timespan: Optional[str], timeout: int = None, retries: int = None) -> ClientRawResponse:
    client = default_sdk_client(LogAnalyticsDataClient, auth_resource="https://api.loganalytics.io/")
    client._deserialize = lambda x,y: None
    if isinstance(workspaces, str):
        workspaces_arg = None
        workspace = workspaces
    else:
        workspaces_arg = realize_sequence(workspaces)
        workspace = workspaces_arg[0]
    query_request = QueryBody(query=query, timespan=timespan, workspaces=workspaces_arg)
    operation_config = {}
    custom_headers = None
    if timeout is not None:
        operation_config['timeout'] = timeout + 5
        custom_headers = { 'Prefer': f'wait={timeout}' }
    if retries is not None:
        operation_config['retries'] = retries
    return client.query(workspace, query_request, custom_headers=custom_headers, raw=True, **operation_config)


def _query_native_by_workspace_chunk(chunked_ids: GroupedChunkList, query_builder, timespan: Optional[str], logger) -> List[dict]:
    logger.info(f'Starting chunked query with {len(chunked_ids)} chunk(s) over {len(chunked_ids.groups)} workspace(s).')
    chunk_index = 0
    total_errors = 0
    responses: List[dict] = []
    for workspace_group in chunked_ids.groups:
        current_chunks = workspace_group.chunks
        current_workspace_id = workspace_group.id
        logger.info(f'Querying {len(current_chunks)} chunk(s) in workspace {current_workspace_id}.')
        for chunk_data in current_chunks:
            logger.debug(f'Querying {len(chunk_data)} resource(s).', resources=chunk_data)
            attempt = 0
            while True:
                kql_query = query_builder(chunk_data)
                query_result = None
                try:
                    query_result = _query_native(kql_query, current_workspace_id, timespan, timeout = 300, retries = 0)
                except ClientRequestError as error:
                    if attempt == 5:
                        raise
                    if isinstance(error.inner_exception, Timeout):
                        logger.warning('Request timed out.')
                    else:
                        logger.debug(f'Request failed: {error}. Waiting 5 seconds...')
                        time.sleep(5)
                if query_result is not None:
                    break
                attempt += 1
                logger.warning(f'Retrying last query (attempt {attempt}/5)')
            data = _parse_raw_response_to_data_dict(query_result, hide_primary_data=True)
            kusto_response = _create_kusto_result(data)
            errors = kusto_response.errors_count
            logger.info(f'Query for chunk {chunk_index + 1}/{len(chunked_ids)} complete with {errors} error(s).')
            if errors:
                total_errors += errors
            responses.append(data)
            chunk_index += 1

    level = logging.ERROR if total_errors > 0 else logging.INFO
    logger.log(level, f'Finished chunked query with {total_errors} errors(s).')
    return responses

