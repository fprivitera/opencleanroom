# SPDX-License-Identifier: Apache-2.0
"""Result values as JSON, shared by the CLI, HTTP and MCP."""
from __future__ import annotations

from fractions import Fraction


def jsonable(value):
    """Fractions become floats at this edge only; dates, decimals and the like become their text."""
    if isinstance(value, Fraction):
        return float(value)
    if isinstance(value, (list, tuple)):
        return [jsonable(v) for v in value]
    if value is None or isinstance(value, (bool, int, float, str)):
        return value
    return str(value)
