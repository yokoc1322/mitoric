"""Template rendering helpers."""

from __future__ import annotations

from pathlib import Path

from jinja2 import Environment, FileSystemLoader, select_autoescape

from mitoric.render.formatters import _format_bytes, _format_float, _format_number
from mitoric.reporting.payload_schema import ReportPayload


def _finalize(value: object) -> object:
    if isinstance(value, float):
        return _format_float(value)
    return value


def render_report(template_path: Path, payload: ReportPayload) -> str:
    env = Environment(
        loader=FileSystemLoader(str(template_path.parent)),
        autoescape=select_autoescape(["html", "xml"]),
        finalize=_finalize,
    )
    env.filters["format_number"] = _format_number
    env.filters["format_bytes"] = _format_bytes
    template = env.get_template(template_path.name)
    return template.render(payload=payload)
