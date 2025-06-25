# dash_app.py

import json
import requests
import logging
from dash import Dash, html, dcc, Input, Output, State, callback_context, no_update, dcc
import dash_bootstrap_components as dbc
from dash_extensions import WebSocket
from dash.exceptions import PreventUpdate

import geo_assistant.components as gac

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

        gac.ChatDrawer()
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
    gac.ChatDrawer.register_callbacks(app, "ws")
    app.run(port=8200)