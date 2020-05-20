from azure.common.credentials import get_cli_profile
from typing import List
from azmeta.access import AzureSubscriptionHandle

class AzureCliContext:
    def __init__(self):
        self._profile = get_cli_profile()

        current_subscription = self._profile.get_subscription()
        all_subscriptions = self._profile.load_cached_subscriptions()
        self._subscriptions = [AzureSubscriptionHandle(s['id'], s['name'], s['tenantId']) for s in all_subscriptions if s['tenantId'] == current_subscription['tenantId']]        

    @property
    def subscriptions(self) -> List[AzureSubscriptionHandle]:
        return self._subscriptions