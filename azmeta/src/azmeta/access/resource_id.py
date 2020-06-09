from typing import NewType


SubscriptionId = NewType('SubscriptionId', str)


def subscription_id(resource_id: str) -> SubscriptionId:
    parts = resource_id.split('/')
    return SubscriptionId(parts[2])