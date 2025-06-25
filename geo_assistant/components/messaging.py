# app.py
from dash import html, dcc

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