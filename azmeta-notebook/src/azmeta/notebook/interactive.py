from IPython import get_ipython, InteractiveShell
from azmeta.access.config import direct

def connect_kqlmagic() -> None:
    ipython: InteractiveShell = get_ipython()
    c = direct()
    cluster = c['azmeta_kusto']['cluster'].as_str()
    database = c['azmeta_kusto']['database'].as_str()
    ipython.run_line_magic("config", "Kqlmagic.auto_popup_schema=False")
    ipython.run_line_magic("load_ext", "Kqlmagic")
    ipython.run_line_magic("kql", f"azuredataexplorer://code;cluster='{cluster}';database='{database}';alias='azmeta' -try_azcli_login")
