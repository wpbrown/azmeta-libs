from abc import ABCMeta, abstractmethod
from typing import List, Union, Sequence
from azmeta.access import AzureSubscriptionHandle, AzureBillingAccount
from azure.core.credentials import AccessToken

class AzmetaResourceContext(metaclass=ABCMeta):
    @property
    @abstractmethod
    def default_subscription(self) -> AzureSubscriptionHandle:
        pass

    @property
    @abstractmethod
    def subscriptions(self) -> List[AzureSubscriptionHandle]:
        pass

    @property
    @abstractmethod
    def default_billing_account(self) -> AzureBillingAccount:
        pass


class AzmetaAuthenticationContext(metaclass=ABCMeta):
    @abstractmethod
    def get_token(self, scopes: Union[Sequence[str],str]) -> AccessToken:
        pass
