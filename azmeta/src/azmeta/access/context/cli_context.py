import json
import os
import subprocess
from functools import cached_property
from typing import List, Union, Sequence

from azure.core.credentials import AccessToken
from azure.identity import AzureCliCredential
from confuse import NotFoundError

from azmeta.access import AzureSubscriptionHandle, AzureBillingAccount
from azmeta.access.billing import get_billing_accounts
from azmeta.access.config import direct
from azmeta.access.context.interface import AzmetaResourceContext, AzmetaAuthenticationContext


class AzureCliResourceContext(AzmetaResourceContext):
    def __init__(self):
        pass

    @cached_property
    def default_subscription(self) -> AzureSubscriptionHandle:
        return next(s for s in self.subscriptions if s.is_default)

    @cached_property
    def subscriptions(self) -> List[AzureSubscriptionHandle]:
        json_data = json.loads(_run_command("az account list"))
        all_subscriptions = [
            AzureSubscriptionHandle(s["id"], s["name"], s["tenantId"], s["isDefault"]) for s in json_data
        ]
        default = next(s for s in all_subscriptions if s.is_default)
        return [s for s in all_subscriptions if s.tenant_id == default.tenant_id]

    @cached_property
    def default_billing_account(self) -> AzureBillingAccount:
        c = direct()
        try:
            return AzureBillingAccount(
                c["azmeta"]["default_billing_scope"]["billing_account"].as_str(), "Unknown", is_default=True
            )
        except NotFoundError:
            all_ids = get_billing_accounts()
            if len(all_ids) == 1:
                return all_ids[0]
            raise Exception("Multiple billing accounts detected. Set default account in the azmeta config.")


CLI_NOT_FOUND = "Azure CLI not found on path"
NOT_LOGGED_IN = "Please run 'az login' to set up an account"


def _run_command(command):
    args = ["/bin/sh", "-c", command]
    try:
        return subprocess.check_output(
            args,
            stderr=subprocess.STDOUT,
            cwd="/bin",
            text=True,
            env=dict(os.environ, AZURE_CORE_NO_COLOR="true"),
        )
    except subprocess.CalledProcessError as ex:
        if ex.returncode == 127 or ex.output.startswith("'az' is not recognized"):
            error = Exception(CLI_NOT_FOUND)
        elif "az login" in ex.output or "az account set" in ex.output:
            error = Exception(NOT_LOGGED_IN)
        else:
            if ex.output:
                message = ex.output
            else:
                message = "Failed to invoke Azure CLI"
            error = Exception(message)

        raise error from ex


class AzureCliAuthenticationContext(AzmetaAuthenticationContext):
    def __init__(self):
        self._credential = AzureCliCredential()

    def get_token(self, scopes: Union[Sequence[str], str]) -> AccessToken:
        return self._credential.get_token(scopes)
