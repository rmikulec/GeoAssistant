# app.py
from dash import html, dcc
import dash_bootstrap_components as dbc


# 1) Subclass html.Div *first*, drop ABC entirely
class Message(html.Div):
    # camelCase, no hyphens or leading spaces
    _base_style = {
        "padding":            "10px 14px",
        "borderRadius":       "18px",
        "maxWidth":           "100%",
        "wordWrap":           "break-word",
        "lineHeight":         1.4,            # unitless OK
    }

    # new: default styling for the <p>
    _p_style = {
        "fontSize": "0.75rem",          # smaller
        "color":    "rgba(0,0,0,0.5)",   # more subtle
        "margin":   "0 0 4px 0",         # tighten spacing
    }

    # override these per‐subclass
    _message_type = "base"
    _style = {}

    def __init__(self, message: str, id: str = None, **kwargs):
        # merge the base + subtype style
        style = {**self._base_style, **self._style}

        # your children
        children = [
            html.P(f"{self._message_type}:", style=self._p_style),
            dcc.Markdown(message),
        ]

        # pass children, id, style, and **any** other props to html.Div
        super().__init__(children=children, id=id, style=style, **kwargs)


class UserMessage(Message):
    _message_type = "user"
    _style = {
        "backgroundColor":         "rgba(146,33,33,0.3)",
        "color":                   "#fff",
        "alignSelf":               "flex-end",
        "borderBottomRightRadius": 2,
        "borderBottomLeftRadius":  10,
        "borderTopLeftRadius":     10,
        "borderTopRightRadius":    10,
    }


class AssistantMessage(Message):
    _message_type = "assistant"
    _style = {
        "backgroundColor":         "rgba(255,255,255,0.4)",
        "backdropFilter":          "blur(6px)",
        "WebkitBackdropFilter":    "blur(6px)",
        "color":                   "#000",
        "alignSelf":               "flex-start",
        "borderBottomLeftRadius":  2,
        "borderBottomRightRadius": 10,
        "borderTopLeftRadius":     10,
        "borderTopRightRadius":    10,
    }


class ReportMessage(html.Div):
    """
    A styled message box for reporting progress.

    - Top: Heading with report name, "report" subheading and book icon
    - Paragraph: updateable text
    - Progress bar: shows progress, turns green on completion, red on error
    """
    _base_style = {
        "padding": "12px",
        "borderRadius": "12px",
        "backgroundColor": "#1f1f1f",
        "color": "#fff",
        "marginBottom": "10px",
    }

    def __init__(
        self,
        report_name: str,
        message: str = "",
        progress: float = None,
        status: str = None,
        id: str = None,
        **kwargs,
    ):
        # Heading with book icon and title
        heading = html.Div(
            [
                html.I(className="fa fa-book me-2"),
                html.Span(report_name, className="h5 mb-0"),
            ],
            className="d-flex align-items-center mb-1",
        )

        # Updateable paragraph
        text = html.P(
            message,
            id=f"{id}-text" if id else None,
            className="mb-2",
        )

        # Determine progress bar parameters
        if status == "complete":
            print("Complete")
            color = "success"
            value = 100
            striped = False
            animated = False
        elif status == "error":
            print("Error")
            color = "danger"
            value = 100
            striped = False
            animated = False
        elif status == "generate":
            print("Generate")
            color = "rgba(146,33,33,0.3)"
            value = (int(progress * 100)-1) if progress is not None else 0
            striped = True
            animated = True
        else:
            print("Other")
            color = "info"
            value = (int(progress * 100)-1) if progress is not None else 0
            striped = False
            animated = True

        # Progress bar
        progress_bar = dbc.Progress(
            value=value,
            id=f"{id}-progress" if id else None,
            striped=striped,
            animated=animated,
            style={"height": "10px"},
            color=color,
        )

        # Assemble children and initialize
        children = [heading, text, progress_bar]
        super().__init__(children=children, id=id, style=self._base_style, key=f"{id}-{status}", **kwargs)
