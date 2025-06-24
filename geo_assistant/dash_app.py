# dash_app.py

import json
from dash import Dash, html, dcc, Input, Output, State, callback_context, no_update
import dash_bootstrap_components as dbc
from dash_extensions import WebSocket
from dash.exceptions import PreventUpdate

from geo_assistant.logging import get_logger
logger = get_logger(__name__)

def create_dash_app(server, initial_figure):
    # ─── Initialize your GeoAgent ────────────────────────────────────────────────

    # ─── Create Dash, mounted under /dash/ ────────────────────────────────────────
    dash_app = Dash(
        __name__,
        requests_pathname_prefix="/dash/",
        external_stylesheets=[dbc.themes.BOOTSTRAP, dbc.icons.FONT_AWESOME],
        suppress_callback_exceptions=True,
    )

    # ─── Layout ───────────────────────────────────────────────────────────────────
    dash_app.layout = html.Div([
        dcc.Graph(
            id="map-graph", 
            config={"displayModeBar": False},
            figure=initial_figure,
            style={"position": "absolute", "top": 0, "left": 0, "right": 0, "bottom": 0},
        ),

        html.Div(
            dbc.Button(
                dcc.Loading(
                    html.I(id="chat-btn-icon", className="fa-solid fa-comments"),
                    id="loading-chat-btn",
                    type="default",
                    color="rgba(255,255,255,0.8)",
                ),
                id="open-chat",
                color="primary",
                size="lg",
                className="glass-button",
            ),
            style={"position":"fixed","top":"15px","right":"15px","zIndex":1000},
        ),

        dbc.Offcanvas(
            html.Div([
                html.H5("Chat", className="mb-1 text-white"),

                html.Div(
                    id="chat-log",
                    className="flex-grow-1 overflow-auto mb-3 glass-chat-log",
                    style={"padding":"5px","whiteSpace":"pre-wrap","color":"#fff"},
                ),

                dbc.InputGroup([
                    dbc.Input(
                        id="chat-input",
                        placeholder="Type your message…",
                        type="text",
                        debounce=True,
                        className="glass-input text-white",
                    ),
                    dbc.Button(
                        html.I(className="fa-solid fa-paper-plane"),
                        id="send-btn",
                        color="light",
                        n_clicks=0,
                        className="glass-button",
                    ),
                ], className="mt-auto"),

                # Plain WebSocket client pointed at your FastAPI /ws endpoint
                WebSocket(id="ws", url="ws://localhost:8000/ws"),
            ],
            style={"height":"100%","display":"flex","flexDirection":"column"}),
            id="chat-drawer",
            is_open=False,
            placement="end",
            className="glass-offcanvas",
            style={"width":"400px","zIndex":"1100"},
        ),
    ])

    # ─── Callbacks ────────────────────────────────────────────────────────────────

    # 1) Toggle the chat drawer
    # toggle offcanvas
    @dash_app.callback(
        Output("chat-drawer", "is_open"),
        Input("open-chat", "n_clicks"),
        State("chat-drawer", "is_open"),
        prevent_initial_call=True,
    )
    def toggle_chat(n, is_open):
        if n:
            return not is_open
        return is_open

    # 2) Single callback for both sending user messages and handling WS messages
    @dash_app.callback(
        # When sending: we push text into the WS via its `send` prop...
        Output("ws", "send"),
        # ...and we immediately append the user message to the log + clear the input
        Output("chat-log", "children"),
        Output("chat-input", "value"),
        Output("map-graph", "figure"),

        # We also listen for incoming WS frames to append analysis/AI updates
        Input("send-btn", "n_clicks"),
        Input("ws", "message"),
        State("chat-input", "value"),
        State("chat-log", "children"),
        prevent_initial_call=True,
    )
    def update_chat(n_clicks, ws_msg, new_message, log):
        log = log or []
        trig = callback_context.triggered_id

        # — User clicked “send”
        if trig == "send-btn":
            if not new_message:
                raise PreventUpdate
            # append user bubble
            log.append(html.Div(new_message, className="chat-message user-message"))
            # send it down the socket to FastAPI
            payload = json.dumps({"type": "user", "message": new_message})
            return payload, log, "", no_update

        # — We received a server‐push update
        if trig == "ws":
            if not ws_msg or "data" not in ws_msg:
                raise PreventUpdate
            payload = json.loads(ws_msg["data"])
            typ  = payload.get("type")
            txt  = payload.get("message", "")
            prog = payload.get("progress")

            if typ == "analysis":
                analysis_id = payload.get("id")
                # build the children for this analysis div
                children = [html.Div(txt, className="analysis-message-text")]
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

                # our stable div id
                div_id = f"analysis-{analysis_id}"

                # try to find an existing entry in log with that id
                existing_idx = next(
                    (i for i, child in enumerate(log)
                    if getattr(child, "props", {}).get("id") == div_id),
                    None
                )
                logger.info(f"Existing id: {existing_idx}")
                new_div = html.Div(children, id=div_id, className="chat-message assistant-message")

                if existing_idx is not None:
                    # replace the old one
                    log[existing_idx] = new_div
                else:
                    # first time: append it
                    log.append(new_div)

            elif typ == "ai_response":
                log.append(html.Div(txt, className="chat-message assistant-message"))
            elif typ == "figure_update":
                figure = json.loads(payload.get("figure"))
                return no_update, no_update, no_update, figure
            else:
                # fallback for other event types
                log.append(html.Div(txt, className="chat-message"))

            return no_update, log, no_update, no_update

        # Otherwise—nothing to do
        raise PreventUpdate

    return dash_app
