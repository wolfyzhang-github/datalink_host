from __future__ import annotations

from functools import lru_cache
from importlib.resources import files


@lru_cache(maxsize=1)
def load_index_html() -> str:
    return files("datalink_host.web").joinpath("index.html").read_text(encoding="utf-8")


INDEX_HTML = load_index_html()
