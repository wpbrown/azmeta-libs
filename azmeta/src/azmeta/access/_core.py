from typing import NamedTuple, Iterable, List


class AzureBillingAccount(NamedTuple):
    id: str
    display_name: str
    is_default: bool

    def resource_id(self):
        return f"/providers/Microsoft.Billing/billingAccounts/{self.id}"


class AzureBillingSection(NamedTuple):
    id: str
    display_name: str
    is_default: bool


class AzureSubscriptionHandle(NamedTuple):
    subscription_id: str
    subscription_name: str
    tenant_id: str
    is_default: bool

    def resource_id(self):
        return f"/subscriptions/{self.subscription_id}"


def list_subscription_ids(subscriptions: Iterable[AzureSubscriptionHandle]) -> List[str]:
    return [s.subscription_id for s in subscriptions]
