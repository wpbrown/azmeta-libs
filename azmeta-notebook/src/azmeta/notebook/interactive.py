from IPython import get_ipython, InteractiveShell
from azmeta.access.config import direct
import confuse

_billing_scope_template = {
    'accounts': confuse.StrSeq(split=False)
}


def connect_kqlmagic() -> None:
    ipython: InteractiveShell = get_ipython()
    c = direct()
    cluster = c['azmeta_kusto']['cluster'].as_str()
    database = c['azmeta_kusto']['database'].as_str()
    try:
        default_billing_scope = c['azmeta']['default_billing_scope'].get(_billing_scope_template)
    except confuse.NotFoundError:
        default_billing_scope = None
        pass

    ipython.run_line_magic("config", "Kqlmagic.auto_popup_schema=False")
    ipython.run_line_magic("load_ext", "Kqlmagic")
    ipython.run_line_magic("kql", f"azuredataexplorer://code;cluster='{cluster}';database='{database}';alias='azmeta' -try_azcli_login")

    if default_billing_scope:
        accounts = default_billing_scope['accounts']
        condition = f"== '{accounts[0]}'" if len(accounts) == 1 else "in ('{}')".format("','".join(accounts))
        azmeta_kql_usage_scope = f"where AccountName {condition}"
    else:
        azmeta_kql_usage_scope = "as __unused__"
    
    ipython.push('azmeta_kql_usage_scope')
