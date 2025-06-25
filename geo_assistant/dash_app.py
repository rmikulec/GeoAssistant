# dash_app.py

import json
import requests
import logging
from dash import Dash, html, dcc, Input, Output, State, no_update
import dash_bootstrap_components as dbc
from dash_extensions import WebSocket
from dash.exceptions import PreventUpdate
import dash_leaflet as dl

import geo_assistant.components as gac

logger = logging.getLogger(__name__)

def create_dash_app(initial_map: dict) -> Dash:
    """
    Create the Dash app using Dash-Leaflet.

    Args:
        initial_map (dict): JSON with keys "center", "zoom", "children"
    """
    app = Dash(
        __name__,
        external_stylesheets=[dbc.themes.BOOTSTRAP, dbc.icons.FONT_AWESOME],
        suppress_callback_exceptions=True,
    )

    # initial children components
    initial_children = [
        getattr(dl, layer["type"])(**layer["props"])
        for layer in initial_map.get("children", [])
    ]

    app.layout = html.Div([
        # WebSocket to your FastAPI /ws
        WebSocket(id="ws", url="ws://localhost:8000/ws"),

        # Click feedback
        html.Div(id="click-output",
                 style={"position": "fixed", "bottom": "10px", "left": "10px",
                        "background": "white", "padding": "5px"}),

        # Hidden store for raw click data (if you want)
        html.Div(id="click-data", style={"display": "none"}),

        # The Dash-Leaflet map
        dl.Map(
            id="map",
            center=initial_map["center"],
            zoom=initial_map["zoom"],
            children=initial_children,
            style={"width": "100%", "height": "100vh"},
        ),

        # Chat drawer toggle button
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
            style={"position": "fixed", "top": "15px", "right": "15px", "zIndex": 1000},
        ),

        # The drawer component (unchanged)
        gac.ChatDrawer()
    ])

    # Toggle chat drawer
    @app.callback(
        Output("chat-drawer", "is_open"),
        Input("open-chat", "n_clicks"),
        State("chat-drawer", "is_open"),
        prevent_initial_call=True,
    )
    def toggle_chat(n, is_open):
        if n:
            return not is_open
        return is_open

    # Update map on WebSocket messages of type "figure_update" (you can rename to "map_update")
    @app.callback(
        Output("map", "center"),
        Output("map", "zoom"),
        Output("map", "children"),
        Input("ws", "message"),
        State("map", "center"),
        State("map", "zoom"),
        State("map", "children"),
        prevent_initial_call=True,
    )
    def update_map(ws_msg, cur_center, cur_zoom, cur_children):
        if not ws_msg or "data" not in ws_msg:
            raise PreventUpdate
        msg = json.loads(ws_msg["data"])
        if msg.get("type") != "figure_update":
            raise PreventUpdate
        payload = msg['figure']
        # rebuild children list
        new_children = [
            getattr(dl, layer["type"])(**layer["props"])
            for layer in payload.get("children", [])
        ]
        logger.info("Map updating via WebSocket…")
        return payload.get("center", cur_center), payload.get("zoom", cur_zoom), new_children

    # Display click coordinates
    @app.callback(
        Output("click-output", "children"),
        Input("map", "click_lat_lng"),
        prevent_initial_call=True,
    )
    def display_click(lat_lng):
        if not lat_lng:
            return "Click on the map…"
        lat, lng = lat_lng
        return f"Lat: {lat:.5f}, Lon: {lng:.5f}"

    return app


if __name__ == "__main__":
    # Fetch the initial map JSON from your Flask/Dash-Leaflet handler
    r = requests.get("http://127.0.0.1:8000/map-figure")
    r.raise_for_status()
    initial_map = r.json()

    app = create_dash_app(initial_map=initial_map)
    # Register the ChatDrawer callbacks
    gac.ChatDrawer.register_callbacks(app, "ws")
    app.run(port=8200)
