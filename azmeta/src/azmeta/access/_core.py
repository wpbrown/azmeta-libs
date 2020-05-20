from typing import NamedTuple, Iterable, List


class AzureSubscriptionHandle(NamedTuple):
    subscription_id: str
    subscription_name: str
    tenant_id: str


def list_subscription_ids(subscriptions: Iterable[AzureSubscriptionHandle]) -> List[str]:
    return [s.subscription_id for s in subscriptions]
