import dataclasses
import re
from typing import List, Iterator


@dataclasses.dataclass
class Span:
    contents: str
    type: str


def format_page(template: str, context: dict) -> Iterator[str]:
    curr_idx = 0
    spans = []
    for marker in re.finditer(r"\{\{([a-zA-Z_][a-zA-Z0-9_ ]+)}}", template):
        spans.append(Span(template[curr_idx : marker.start()], "text"))
        spans.append(Span(marker.group(1), "marker"))
        curr_idx = marker.end()
    spans.append(Span(template[curr_idx:], "text"))

    return _format_span_list(spans, context)


def _format_span_list(spans: List[Span], context: dict):
    spans = iter(spans)
    while True:
        try:
            span = next(spans)
        except StopIteration:
            break

        if span.type == "text":
            yield span.contents
        elif span.type == "marker":
            if span.contents.startswith("foreach "):
                inner_spans = []
                while True:
                    inner_span = next(spans)
                    if (
                        inner_span.type == "marker"
                        and inner_span.contents == "end " + span.contents
                    ):
                        break
                    inner_spans.append(inner_span)

                key = span.contents[len("foreach ") :]
                for item in context[key]:
                    merged = context.copy()
                    merged.update(item)
                    yield from _format_span_list(inner_spans, merged)
            elif " " not in span.contents:
                yield context[span.contents]
            else:
                raise ValueError(f"Invalid marker: `{span.contents}`")
