from typing import Any


def quote_string(s: Any) -> Any:
    """Add correct quotes to string for our __repr__"""
    if  not isinstance(s, str):
        return s
    if chr(34) in s and chr(39) in s:
        return "'''" + s + "'''"
    if chr(39) in s:
        return chr(34) + s + chr(34)
    return chr(39) + s + chr(39)