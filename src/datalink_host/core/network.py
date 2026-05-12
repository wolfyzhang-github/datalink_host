from __future__ import annotations

import socket


_WILDCARD_HOSTS = {"", "0.0.0.0", "::", "::0"}


def access_host_for_bind_host(bind_host: str) -> str:
    host = str(bind_host or "").strip()
    if host not in _WILDCARD_HOSTS:
        return host

    detected = _detect_primary_ipv4()
    if detected:
        return detected
    return "127.0.0.1"


def _detect_primary_ipv4() -> str | None:
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:
            sock.connect(("8.8.8.8", 80))
            address = sock.getsockname()[0]
            if _is_usable_ipv4(address):
                return address
    except OSError:
        pass

    try:
        addresses = socket.gethostbyname_ex(socket.gethostname())[2]
    except OSError:
        return None
    for address in addresses:
        if _is_usable_ipv4(address):
            return address
    return None


def _is_usable_ipv4(address: str) -> bool:
    return bool(address) and not address.startswith("127.") and address != "0.0.0.0"
