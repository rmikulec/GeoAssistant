# dash_app.py

import json
import requests
import logging
from dash import Dash, html, dcc, Input, Output, State, callback_context, no_update
import dash_bootstrap_components as dbc
from dash_extensions import WebSocket
from dash.exceptions import PreventUpdate


logger = logging.getLogger(__name__)

def create_dash_app(initial_figure):
    # ─── Initialize your GeoAgent ────────────────────────────────────────────────

    # ─── Create Dash, mounted under /dash/ ────────────────────────────────────────
    dash_app = Dash(
        __name__,
        #requests_pathname_prefix="/dash/",
        external_stylesheets=[dbc.themes.BOOTSTRAP, dbc.icons.FONT_AWESOME],
        suppress_callback_exceptions=True,
    )

    # ─── Layout ───────────────────────────────────────────────────────────────────
    dash_app.layout = html.Div([
        # Plain WebSocket client pointed at your FastAPI /ws endpoint
        WebSocket(id="ws", url="ws://localhost:8000/ws"),
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

    @dash_app.callback(
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
    
        # ─── Helper: insert or replace an “analysis” div ───────────────────────────
    def _upsert_analysis(log, payload):
        """
        Given the existing list `log` and the WS payload, return a brand-new list
        where a <Div id="analysis-{id}"> is replaced if present, or appended otherwise.
        """
        aid  = str(payload["id"])
        txt  = payload.get("message", "")
        prog = payload.get("progress")

        # 1) Build the new analysis Div
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
        new_div = html.Div(
            children,
            id=f"analysis-{aid}",
            className="chat-message assistant-message",
        )

        # 2) Helper to extract an existing child's id
        def _get_id(child):
            # Dash component: use .id
            if hasattr(child, "id"):
                return child.id
            # plain-dict serialization: look in props
            return child.get("props", {}).get("id")

        desired_id = f"analysis-{aid}"
        new_log = []
        replaced = False

        # 3) Iterate and replace if we find the same id
        for child in log or []:
            existing_id = _get_id(child)
            # DEBUG: you can uncomment the next line to see what's in your log
            # logger.info(f"Child {type(child)} has id={existing_id!r}")

            if existing_id == desired_id:
                new_log.append(new_div)
                replaced = True
            else:
                new_log.append(child)

        # 4) If we never found it, append at the end
        if not replaced:
            new_log.append(new_div)

        return new_log



# ——————————————————————————————————————————————————————————————————————
    # 1) Only update the chat-log
    @dash_app.callback(
        Output("chat-log", "children"),
        Input("ws", "message"),
        State("chat-log", "children"),
        prevent_initial_call=True,
    )
    def update_chat_log(ws_msg, log):
        if not ws_msg or "data" not in ws_msg:
            raise PreventUpdate
        
        payload = json.loads(ws_msg["data"])
        typ = payload.get("type")
        if typ == "figure_update":
            raise PreventUpdate
        logger.info(f"Message Recieved: {ws_msg}")

        text    = payload.get("message", "")
        log      = list(log or [])

        if typ == "analysis":
            return _upsert_analysis(log, payload)

        if typ == "ai_response":
            log.append(html.Div(text, className="chat-message assistant-message"))
            return log

        # catch-all for other small messages
        log.append(html.Div(text, className="chat-message"))
        return log

    # ——————————————————————————————————————————————————————————————————————
    # 2) Only update the figure
    @dash_app.callback(
        Output("map-graph", "figure"),
        Input("ws", "message"),
        State("map-graph", "figure"),
        prevent_initial_call=True,
    )
    def update_map_figure(ws_msg, current_fig):
        if not ws_msg or "data" not in ws_msg:
            raise PreventUpdate

        payload = json.loads(ws_msg["data"])
        if payload.get("type") != "figure_update":
            # no_change for non-figure messages
            raise PreventUpdate
        logger.info('Map updating...')
        return json.loads(payload["figure"])



    return dash_app


if __name__ == "__main__":
    initial_figure = requests.get(url="http://127.0.0.1:8000/map-figure")
    app = create_dash_app(initial_figure=initial_figure.json())
    app.run(port=8200)