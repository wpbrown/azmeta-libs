from typing import Optional, Type, TypeVar
from azmeta.access.context import default_resource_context, default_authentication_context
from inspect import getfullargspec


T = TypeVar('T')


def default_sdk_client(client_class: Type[T], auth_resource: Optional[str] = None, subscription_id: Optional[str] = None) -> T:
    resource_context = default_resource_context()
    auth_context = default_authentication_context()

    if subscription_id is None:    
        subscription_id = resource_context.default_subscription.subscription_id
    credential = _SdkCredential(auth_context, auth_resource if auth_resource else "https://management.core.windows.net/")
    parameters = {
        'subscription_id': subscription_id,
        'credentials': credential,
        'credential': credential
    }

    return _instantiate_client(client_class, **parameters)


def _instantiate_client(client_class, **kwargs):
    args = getfullargspec(client_class.__init__).args
    for key in ['subscription_id', 'tenant_id', 'base_url', 'credential', 'credentials']:
        if key not in kwargs:
            continue
        if key not in args:
            del kwargs[key]

    return client_class(**kwargs)


class _SdkCredential:
    def __init__(self, credential, resource: str):
        self._credential = credential
        self._resource = resource

    def get_token(self, *scopes):
        return self._credential.get_token(*scopes)

    def signed_session(self, session=None):
        token = self.get_token(self._resource)
        header = f"Bearer {token.token}"
        session.headers['Authorization'] = header
        return session