
from .interface import AzmetaResourceContext, AzmetaAuthenticationContext


def default_resource_context() -> AzmetaResourceContext:
    from .cli_context import AzureCliResourceContext
    return AzureCliResourceContext()


def default_authentication_context() -> AzmetaAuthenticationContext:
    from .cli_context import AzureCliAuthenticationContext
    return AzureCliAuthenticationContext()

