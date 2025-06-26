"""
Holds the ChatDrawer component, which will hold and update the states of messages recieved from
    the websocket.

To use, you must call `CardDrawer.register_callbacks`. This will properly hook up all callbacks
    needed for this compoonent to function properly
"""
import json
import uuid
import json
import json
from typing import Any, Dict, List, Optional, Union

from dash import Dash, Input, Output, State, MATCH, ALL, html, dcc
from dash.exceptions import PreventUpdate
import dash_bootstrap_components as dbc

import geo_assistant.components as gac
from geo_assistant.components import css



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
        **css.FROSTED_CONTAINER,
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
    _input_style = css.FROSTED_INPUT
    _button_style = css.FROSTED_INPUT

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
    A complete chat panel (header + log + input) that
    stores ALL messages in one Store and re-renders.
    """
    DEFAULT_ID    = "chat-drawer"
    DEFAULT_TITLE = "Chat"
    DEFAULT_PLACEMENT = "end"
    _drawer_style = {"width":"400px","zIndex":"1100", **css.FROSTED_CONTAINER}

    def __init__(self, id: str = DEFAULT_ID, **kwargs) -> None:
        header = html.H5(self.DEFAULT_TITLE, className="mb-1 text-white")
        log    = ChatLog()
        entry  = ChatInputGroup()

        body = html.Div(
            [
                # <--- this holds **every** payload, in order
                dcc.Store(id="message-store", data=[]),
                header,
                log,
                entry,
            ],
            style={"height":"100%","display":"flex","flexDirection":"column"},
        )

        super().__init__(
            children=body,
            id=id,
            placement=self.DEFAULT_PLACEMENT,
            style=self._drawer_style,
            className="glass-offcanvas",
            **kwargs,
        )

    @classmethod
    def register_callbacks(cls, app: Dash, ws_id: str = "ws") -> None:
        """
        Registers the callbacks to a DashApp with a websocket. This component will monitor the
            websocket for differnt message types, and update the log accordingly

        Args:
            - app (Dash): The dash application holding the component. MUST have a Websocket
                component as well
            - ws_id (str): The ID of the Websocket component
        """
        @app.callback(
            Output("message-store", "data"),
            Input(ws_id, "message"),
            State("message-store", "data"),
            prevent_initial_call=True,
        )
        def collect_all(ws_msg: Optional[Dict[str, Any]], store: List[Dict]) -> List[Dict]:
            """
            Call back that collects all data in the message store, and builds it accordingly.
                This is needed in order to replace stale components with new ones.
            """
            if not ws_msg or "data" not in ws_msg:
                raise PreventUpdate

            payload = json.loads(ws_msg["data"])
            typ = payload.get("type")

            # Build a stable `uid`:
            if typ == "analysis":
                # use the analysis.id as the uid so updates replace
                uid = str(payload["id"])
                # drop any old analysis with the same id
                store = [
                    p for p in (store or [])
                    if not (p.get("type")=="analysis" and str(p.get("id"))==uid)
                ]
            else:
                # brand-new for user/assistant/other
                uid = str(uuid.uuid4())

            payload["uid"] = uid
            return (store or []) + [payload]

        # 2) Re-render the entire chat-log on any store change
        @app.callback(
            Output(ChatLog.DEFAULT_ID, "children"),
            Input("message-store", "data"),
        )
        def render_all(store: List[Dict]) -> List[Any]:
            """
            Callback to render all messages in the log. Will recreate any components that require
                an active state.
            """
            children: List[Any] = []
            for p in store or []:
                typ = p.get("type")
                uid = p["uid"]

                if typ == "analysis":
                    children.append(
                        gac.ReportMessage(
                            report_name="Analysis",
                            query=p.get("query",""),
                            step=p.get("step", ""),
                            progress=p.get("progress"),
                            status=p.get("status"),
                            id=uid,
                        )
                    )
                elif typ in ("ai_response","assistant_message"):
                    children.append(
                        gac.AssistantMessage(
                            p.get("message",""),
                            id={"type":"assistant-msg","id":uid}
                        )
                    )
                elif typ in ("user_message","user"):
                    children.append(
                        gac.UserMessage(
                            p.get("message",""),
                            id={"type":"user-msg","id":uid}
                        )
                    )

            return children



        @app.callback(
            Output("ws", "send"),
            Output("chat-input", "value"),
            Input("send-btn", "n_clicks"),
            State("chat-input", "value"),
            prevent_initial_call=True,
        )
        def send_user_message(n_clicks, message):
            """
            Callback to send any text to the websocket
            """
            if not message:
                raise PreventUpdate
            payload = json.dumps({"type": "user", "message": message})
            # send text, then clear the input
            return payload, ""