from azure.common.client_factory import get_client_from_cli_profile
from azure.mgmt.advisor import AdvisorManagementClient
from azure.mgmt.advisor.models import ResourceRecommendationBase
from typing import Iterable, Union, Iterable, Dict, List
from itertools import chain

def load_resize_recommendations(subscriptions: Union[str, Iterable[str]]) -> Dict[str,ResourceRecommendationBase]:
    target_subscriptions: Iterable[str] = [subscriptions] if isinstance(subscriptions, str) else subscriptions

    def get_iter_for_sub(subscription: str) ->  Iterable[ResourceRecommendationBase]:
        client: AdvisorManagementClient = get_client_from_cli_profile(AdvisorManagementClient, subscription_id=subscription)
        return client.recommendations.list(filter="Category eq 'Cost'")
    
    recommendations = chain.from_iterable(get_iter_for_sub(s) for s in target_subscriptions)
    vm_resize_type_id = 'e10b1381-5f0a-47ff-8c7b-37bd13d7c974'
    return {_trim_resource_id(x.id).lower():x for x in recommendations if x.recommendation_type_id == vm_resize_type_id}
        

def _trim_resource_id(id: str) -> str:
    trim_at = id.rindex('/providers/Microsoft.Advisor/recommendations')
    return id[0:trim_at]