from typing import Dict, NamedTuple
from decimal import Decimal
import json

class AzurePriceRecord(NamedTuple):
    pay_as_you_go: Dict[str, Decimal]
    reserved_1year: Dict[str, Decimal]
    reserved_3year: Dict[str, Decimal]


def load_vm_price_records() -> Dict[str, AzurePriceRecord]:
    with open('/home/will/dev/azure-metadata/azuremeta_access/data.json') as fd:
        data = json.load(fd)
    
    for name, detail in data['offers'].items():
        if not name.startswith('linux-'):
            continue

        print(name)

    print(data)


_location_map = {
    "us-east-2": "eastus2",
    "us-south-central": "southcentralus",
    "australia-east": "australiaeast",
    "europe-west": "westeurope",
    "canada-central": "canadacentral",
    "us-east": "eastus",
    "us-central": "centralus",
    "us-west": "westus"
}