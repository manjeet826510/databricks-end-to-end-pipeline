from configs.config import cfg

def abfss_path(container: str, sub_path: str = "") -> str:
    base = (
        f"abfss://{cfg['containers'][container]}"
        f"@{cfg['storage_account']}.dfs.core.windows.net"
    )
    return f"{base}/{sub_path}" if sub_path else base


def checkpoint_path(layer: str, name: str) -> str:
    return abfss_path(
        layer,
        f"{cfg['checkpoint_base']}/{name}"
    )