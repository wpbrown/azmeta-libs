from datetime import datetime, timedelta, date
from azmeta.access.utils.sdk import default_sdk_client
from azure.mgmt.costmanagement import CostManagementClient
from azure.mgmt.billing import BillingManagementClient
from azure.mgmt.billing.models import BillingPeriod
from azmeta.access import AzureBillingAccount, AzureSubscriptionHandle
from typing import List, Sequence, Tuple, Union
from enum import Enum
from msrest.pipeline import ClientRawResponse
from pandas import DataFrame
from .kusto import KustoColumnDescriptor, kusto_data_to_dataframe
import json
import itertools

from azure.mgmt.costmanagement.models import (
    QueryDefinition,
    QueryTimePeriod,
    QueryDataset,
    TimeframeType,
    ExportType,
    QueryAggregation,
    QueryGrouping,
    QueryFilter,
    QueryColumnType,
    QueryComparisonExpression,
    QueryResult,
)

# Monkey Patch Bug in API Swagger
QueryFilter._attribute_map["dimension"]["key"] = "dimensions"


def get_billing_accounts() -> List[AzureBillingAccount]:
    billing_client = default_sdk_client(BillingManagementClient)
    service_client = billing_client._client
    url = service_client.format_url("/providers/Microsoft.Billing/billingAccounts")
    query_parameters = {"api-version": "2019-10-01-preview"}
    request = service_client.get(url, query_parameters)
    response = service_client.send(request, stream=False)
    if response.status_code != 200:
        raise Exception("Failed to enumerate billing accounts.")
    raw_accounts = json.loads(response.content)["value"]
    return [
        AzureBillingAccount(a["name"], a["properties"]["displayName"], is_default=True)
        for a in raw_accounts
        if a.get("properties", {}).get("agreementType") == "EnterpriseAgreement"
    ]


def get_billing_periods(limit: int = 12) -> List[BillingPeriod]:
    billing_client = default_sdk_client(BillingManagementClient)
    return list(itertools.islice(billing_client.billing_periods.list(top=limit), limit))


def get_last_closed_billing_period() -> BillingPeriod:
    periods = iter(get_billing_periods(limit=3))
    today = date.today()
    active_period = next(periods)
    while today < (active_period.billing_period_end_date + timedelta(days=5)):
        active_period = next(periods)
    return active_period


def billing_period_to_full_day_timespan(period: BillingPeriod) -> Tuple[datetime, datetime]:
    return full_day_timespan(period.billing_period_start_date, period.billing_period_end_date)


def full_day_timespan(
    begin: Union[datetime, date], end: Union[datetime, date, None] = None, end_midnight: bool = False
) -> Tuple[datetime, datetime]:
    if end is None:
        end = begin

    if end_midnight:
        end += timedelta(days=1)
        end_time = (0, 0, 0)
    else:
        end_time = (23, 59, 59)

    begin = datetime(*(begin.timetuple()[:3]))
    end = datetime(*(end.timetuple()[:3]), *end_time)
    return (begin, end)


class GranularityType(str, Enum):
    none = "None"
    daily = "Daily"
    monthly = "Monthly"
    accumulated = "Accumulated"


class ChargeType(Enum):
    usage = "usage"
    purchase = "purchase"
    refund = "refund"
    unused_reservation = "unusedreservation"


class GroupByBase(object):
    def clause(self) -> QueryGrouping:
        return QueryGrouping(type=self._column_type, name=self._value)


class GroupByTag(GroupByBase):
    _column_type: QueryColumnType = QueryColumnType.tag

    def __init__(self, tag_name: str):
        self._value = tag_name


class GroupByColumn(GroupByBase):
    _column_type: QueryColumnType = QueryColumnType.dimension

    def __init__(self, column_name: str):
        self._value = column_name


def query_cost_native(
    scope: Union[AzureBillingAccount, AzureSubscriptionHandle], query: QueryDefinition, max_pages: int = 10
) -> List[QueryResult]:
    return _query_cost_native(scope, query, max_pages)


def query_cost_dataframe(
    scope: Union[AzureBillingAccount, AzureSubscriptionHandle], query: QueryDefinition, max_pages: int = 10
) -> DataFrame:
    responses = _query_cost_native(scope, query, max_pages)

    columns = [
        KustoColumnDescriptor(c.name, _COST_MANAGEMENT_TO_KUSTO_TYPE_MAP[c.type])
        for c in responses[0].columns
    ]
    rows = itertools.chain.from_iterable(r.rows for r in responses)

    return kusto_data_to_dataframe(columns, rows)


_COST_MANAGEMENT_TO_KUSTO_TYPE_MAP = {"Number": "real", "String": "string"}


def _query_cost_native(
    scope: Union[AzureBillingAccount, AzureSubscriptionHandle], query: QueryDefinition, max_pages: int
) -> List[QueryResult]:
    client = default_sdk_client(CostManagementClient)
    raw_result: ClientRawResponse = client.query.usage(scope.resource_id(), query, raw=True)
    result = raw_result.output
    results = [result]
    next_link = result.next_link

    if next_link:
        pages = 1
        headers = raw_result.response.request.headers
        body = json.loads(raw_result.response.request.body)
        service_client = client._client
        while next_link:
            if pages == max_pages:
                raise Exception("More results remain after max pages of cost query.")
            request = service_client.post(next_link, headers=headers, content=body)
            response = service_client.send(request, stream=False)
            if response.status_code != 200:
                raise Exception("Failed to get next page of cost query.")
            result = client.query._deserialize('QueryResult', response)
            next_link = result.next_link
            results.append(result)
            pages += 1

    return results


def create_cost_query(
    timeframe: Union[TimeframeType, Tuple[datetime, Union[datetime, timedelta]]],
    cost_type: ExportType = ExportType.amortized_cost,
    granularity: GranularityType = GranularityType.none,
    grouping: Union[List[GroupByBase], GroupByBase, None] = None,
    filter: QueryFilter = None,
) -> QueryDefinition:
    if isinstance(timeframe, TimeframeType):
        if timeframe == TimeframeType.custom:
            raise ValueError(
                "Instead of custom timeframe type, supply a tuple of start end datetime or start datetime and timedelta."
            )
        time_period = None
    else:
        from_property, to_property = timeframe
        timeframe = TimeframeType.custom
        time_period = QueryTimePeriod(
            from_property=from_property,
            to=to_property if isinstance(to_property, datetime) else (from_property + to_property),
        )

    if grouping:
        if isinstance(grouping, GroupByBase):
            grouping = [grouping]

        grouping = [g.clause() for g in grouping]

    return QueryDefinition(
        type=cost_type,
        timeframe=timeframe,
        time_period=time_period,
        dataset=QueryDataset(
            granularity=granularity,
            aggregation={"totalCost": QueryAggregation(name="Cost")},
            grouping=grouping,
            filter=filter,
        ),
    )


def create_basic_filter(
    resource_types: Union[List[str], str, None] = None, resource_ids: Union[List[str], str, None] = None, charge_type: ChargeType = ChargeType.usage
) -> QueryFilter:
    if isinstance(resource_types, str):
        resource_types = [resource_types]
    
    if isinstance(resource_ids, str):
        resource_ids = [resource_ids]

    filters = [
        QueryFilter(
            dimension=QueryComparisonExpression(name="ChargeType", operator="In", values=[charge_type.value],)
        )
    ]

    if resource_types:
        filters.append(
            QueryFilter(
                dimension=QueryComparisonExpression(
                    name="ResourceType", operator="In", values=resource_types,
                )
            )
        )
    if resource_ids:
        filters.append(
            QueryFilter(
                dimension=QueryComparisonExpression(
                    name="ResourceId", operator="In", values=resource_ids,
                )
            )
        )

    return QueryFilter(and_property=filters)

