# components.py
import json
from typing import Any, Dict, List, Optional, Union

from dash import Dash, Input, Output, State, html
from dash.exceptions import PreventUpdate
import dash_bootstrap_components as dbc

import geo_assistant.components as gac


# Shared frosted-glass styles
FROSTED_CONTAINER: Dict[str, Any] = {
    "backgroundColor": "rgba(255,255,255,0.3)",
    "backdropFilter": "blur(8px)",
    "WebkitBackdropFilter": "blur(8px)",
    "border": "1px solid rgba(255,255,255,0.6)",
    "borderRadius": "5px",
}

FROSTED_INPUT: Dict[str, Any] = {
    "backgroundColor": "rgba(255,255,255,0.25)",
    "backdropFilter": "blur(6px)",
    "WebkitBackdropFilter": "blur(6px)",
    "border": "1px solid rgba(255,255,255,0.5)",
    "borderRadius": "4px",
}


class ChatLog(html.Div):
    """
    A scrollable, frosted-glass chat log.
    """
    DEFAULT_ID = "chat-log"
    _base_style = {
        "flexGrow": 1,
        "overflowY": "auto",
        "padding": "5px",
        "whiteSpace": "pre-wrap",
        "color": "#fff",
        **FROSTED_CONTAINER,
    }

    def __init__(
        self,
        id: str = DEFAULT_ID,
        children: Optional[List[Any]] = None,
        style: Optional[Dict[str, Any]] = None,
        **kwargs
    ) -> None:
        final_style = {**self._base_style, **(style or {})}
        super().__init__(children=children or [], id=id, style=final_style, **kwargs)


class ChatInputGroup(dbc.InputGroup):
    """
    A frosted-glass text input plus send button.
    """
    DEFAULT_INPUT_ID = "chat-input"
    DEFAULT_BUTTON_ID = "send-btn"
    DEFAULT_PLACEHOLDER = "Type your message…"
    _input_style = FROSTED_INPUT
    _button_style = FROSTED_INPUT

    def __init__(
        self,
        input_id: str = DEFAULT_INPUT_ID,
        button_id: str = DEFAULT_BUTTON_ID,
        placeholder: str = DEFAULT_PLACEHOLDER,
        **kwargs
    ) -> None:
        inp = dbc.Input(
            id=input_id,
            placeholder=placeholder,
            type="text",
            debounce=True,
            className="glass-input text-white",
            style=self._input_style,
        )
        btn = dbc.Button(
            html.I(className="fa-solid fa-paper-plane"),
            id=button_id,
            color="light",
            n_clicks=0,
            className="glass-button",
            style=self._button_style,
        )
        super().__init__([inp, btn], className="mt-auto", **kwargs)


class ChatDrawer(dbc.Offcanvas):
    """
    A complete chat panel (header + log + input).
    """
    DEFAULT_ID = "chat-drawer"
    DEFAULT_TITLE = "Chat"
    DEFAULT_PLACEMENT = "end"
    _drawer_style = {"width": "400px", "zIndex": "1100", **FROSTED_CONTAINER}

    def __init__(
        self,
        id: str = DEFAULT_ID,
        title: str = DEFAULT_TITLE,
        is_open: bool = False,
        placement: str = DEFAULT_PLACEMENT,
        **kwargs
    ) -> None:
        header = html.H5(title, className="mb-1 text-white")
        log = ChatLog()
        entry = ChatInputGroup()

        body = html.Div(
            [header, log, entry],
            style={"height": "100%", "display": "flex", "flexDirection": "column"},
        )

        super().__init__(
            children=body,
            id=id,
            is_open=is_open,
            placement=placement,
            className="glass-offcanvas",
            style=self._drawer_style,
            **kwargs
        )

    @staticmethod
    def _upsert_analysis(
        existing: List[Union[html.Div, Dict[str, Any]]],
        payload: Dict[str, Any],
    ) -> List[Union[html.Div, Dict[str, Any]]]:
        """Insert or replace the analysis message + progress bar."""
        aid = str(payload["id"])
        msg = payload.get("message", "")
        prog = payload.get("progress")

        # build assistant message
        children = [gac.AssistantMessage(msg, id=aid)]
        if prog is not None:
            children.append(
                dbc.Progress(
                    value=int(prog * 100),
                    max=100,
                    striped=True,
                    animated=True,
                    style={"height": "6px", "marginTop": "4px"},
                    color="info",
                )
            )

        new_div = html.Div(
            children,
            id=f"analysis-{aid}",
            className="chat-message assistant-message",
        )

        # try to replace existing, else append
        desired = f"analysis-{aid}"
        new_log = []
        for child in existing or []:
            cid = getattr(child, "id", child.get("props", {}).get("id"))
            if cid == desired:
                new_log.append(new_div)
                break
            new_log.append(child)
        else:
            # not found: append at end
            new_log.append(new_div)
        return new_log

    @classmethod
    def register_callbacks(cls, app: Dash, ws_component_id: str = "ws") -> None:
        """
        Hooks up the WebSocket->chat-log logic.
        :param ws_component_id: the id of your dcc.WebSocket component
        """
        @app.callback(
            Output(ChatLog.DEFAULT_ID, "children"),
            Input(ws_component_id, "message"),
            State(ChatLog.DEFAULT_ID, "children"),
            prevent_initial_call=True,
        )
        def _on_ws_message(ws_msg: Optional[Dict[str, Any]], log: List[Any]) -> List[Any]:
            if not ws_msg or "data" not in ws_msg:
                raise PreventUpdate

            payload = json.loads(ws_msg["data"])
            typ = payload.get("type")
            text = payload.get("message", "")

            # skip figure updates
            if typ == "figure_update":
                raise PreventUpdate

            if typ == "analysis":
                return cls._upsert_analysis(log, payload)

            # map to the right message component
            if typ == "ai_response":
                comp = gac.AssistantMessage(text, id=f"message-{len(log)+1}")
            elif typ == "user_message":
                comp = gac.UserMessage(text, id=f"message-{len(log)+1}")
            else:
                comp = html.Div(text, className="chat-message", id=f"message-{len(log)+1}")

            log = list(log or [])
            comp.id = f"{typ}-{len(log)+1}"
            log.append(comp)
            return log

        @app.callback(
            Output("ws", "send"),
            Output("chat-input", "value"),
            Input("send-btn", "n_clicks"),
            State("chat-input", "value"),
            prevent_initial_call=True,
        )
        def send_user_message(n_clicks, message):
            if not message:
                raise PreventUpdate
            payload = json.dumps({"type": "user", "message": message})
            # send text, then clear the input
            return payload, ""