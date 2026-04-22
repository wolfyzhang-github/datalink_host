from __future__ import annotations

from functools import lru_cache
from importlib.resources import files
from pathlib import Path


def web_root_path() -> Path:
    return Path(__file__).resolve().parent.parent / "web"


def web_assets_path() -> Path:
    return web_root_path() / "assets"


def bundled_web_asset_names() -> tuple[str, ...]:
    assets_dir = web_assets_path()
    if not assets_dir.is_dir():
        return ()
    return tuple(sorted(path.name for path in assets_dir.iterdir() if path.is_file()))


@lru_cache(maxsize=1)
def load_index_html() -> str:
    return files("datalink_host.web").joinpath("index.html").read_text(encoding="utf-8")


INDEX_HTML = load_index_html()
