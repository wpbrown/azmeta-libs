from azmeta.access.utils.sdk import default_sdk_client
from azure.mgmt.reservations import AzureReservationAPI
from azure.mgmt.reservations.models import ReservationResponse
from azure.mgmt.reservations.operations._reservation_operations import ReservationOperations
from typing import List, Iterable, Tuple, Any
from pandas import DataFrame
import pandas



def reservations_native() -> List[ReservationResponse]:
    return _reservations_native()


def reservations_dataframe() -> DataFrame:
    responses = _reservations_native()

    def make_record(response: ReservationResponse):
        columns: dict = response.as_dict()
        columns['sku'] = columns['sku']['name']
        columns.update(columns['properties'])
        columns['term'] = _convert_to_timedelta(columns['term'])
        del columns['properties']
        del columns['etag']
        del columns['type']
        return columns

    records = [make_record(response) for response in responses]
    return DataFrame(records).astype({
            'expiry_date': 'datetime64[ns, UTC]',
            'effective_date_time': 'datetime64[ns, UTC]',
            'last_updated_date_time': 'datetime64[ns, UTC]',
        }).convert_dtypes().astype({'term': 'timedelta64[ns]'})
    

def _reservations_native() -> List[ReservationResponse]:
    api = default_sdk_client(AzureReservationAPI)

    try:
        original_url = ReservationOperations.list.metadata['url']
        # Monkey Patch the Python SDK. A wrapper for this functionality is missing.
        ReservationOperations.list.metadata['url'] = '/providers/Microsoft.Capacity/reservations'
        # list() input is ignored due to patch above.
        all_results = list(api.reservation.list('*'))
    finally:
        ReservationOperations.list.metadata['url'] = original_url
    
    return all_results


def _convert_to_timedelta(psuedo_iso: str) -> int:
    if psuedo_iso[0] != 'P' or psuedo_iso[2] != 'Y':
        raise 'Unsupported'

    return _ns_per_year * int(psuedo_iso[1])


_ns_per_year = 86400000000000 * 365