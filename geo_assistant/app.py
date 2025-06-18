import dash
import logging
import asyncio
import pathlib
from dash import html, dcc, Input, Output, State, no_update
import dash_bootstrap_components as dbc

from geo_assistant.handlers import MapHandler, DataHandler
from geo_assistant.agent import GeoAgent


# Initialize Classes
logger = logging.getLogger(__name__)
# Set up app
app = dash.Dash(__name__, external_stylesheets=[dbc.themes.BOOTSTRAP, dbc.icons.FONT_AWESOME])
server = app.server


# Set up geo-assistant
agent = GeoAgent(
    map_handler=MapHandler(
        table_name="parcels",
        table_id="public.parcels"
    ),
    data_handler=DataHandler(
        table_name="parcels"
    ),
)


app.layout = html.Div([
    # full‐screen graph
    dcc.Graph(
        id="map-graph",
        figure=agent.map_handler.figure,
        style={"position": "absolute", "top": 0, "left": 0, "right": 0, "bottom": 0},
        config={'displayModeBar': False}
    ),

    # chat‐open button
    html.Div(
        dbc.Button(
            dcc.Loading(
                html.I(id="chat-btn-icon", className='fa-solid fa-comments'),
                id="loading-chat-btn",
                type="default"
            ),
            id="open-chat", color="primary", size="lg"
        ),
        style={"position": "fixed", "top": "15px", "right": "15px", "zIndex": 1000},
    ),

    # offcanvas chat
    dbc.Offcanvas(
        html.Div([
            html.H5("Chat", className="mb-3"),

            html.Div(
                id="chat-log",
                className="flex-grow-1 overflow-auto mb-3",
                style={
                    "backgroundColor": "#f8f9fa",
                    "padding": "10px",
                    "borderRadius": "4px",
                    "border": "1px solid #dee2e6",
                    "whiteSpace": "pre-wrap"
                }
            ),


            # input + send button
            dbc.InputGroup(
                [
                    dbc.Input(
                        id="chat-input",
                        placeholder="Type your message…",
                        type="text",
                        debounce=True,
                    ),
                    dbc.Button(
                        html.I(className="fa-solid fa-paper-plane"),
                        id="send-btn",
                        color="primary",
                        n_clicks=0,
                    ),
                ],
                className="mt-auto",
            ),
        ],
        style={"height": "100%", "display": "flex", "flexDirection": "column"}
        ),
        id="chat-drawer",
        title="",
        is_open=False,
        placement="end",
        style={"width": "350px"}
    )
])


# toggle offcanvas
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


@app.callback(
    Output("chat-log",   "children"),
    Output("chat-input", "value"),
    Output("map-graph",  "figure"),
    Output("chat-btn-icon",  "children"),   # ← new!
    Input("send-btn",    "n_clicks"),
    State("chat-input",  "value"),
    State("chat-log",    "children"),
    running=[
        (Output("chat-input", "disabled"), True, False),
        (Output("send-btn",    "disabled"), True, False),
        (
            Output("send-btn", "children"),
            "Thinking…",  
            html.I(className="fa-solid fa-paper-plane")
        ),
    ]
)
def send_message(n_clicks, new_message, existing_log):
    if not new_message:
        # no change if input is empty
        return existing_log, ""
    
    # start from empty list if None
    log = existing_log or []
    
    # wrap each message in a div (you can customize the className/style)
    log.append(html.Div(f"User: {new_message}", className="mb-2"))

    ai_response = asyncio.run(agent.chat(new_message))
    log.append(html.Div(f"GeoAssistant: {ai_response}", className="mb-2"))
    
    # clear the input after sending
    return log, "", agent.map_handler.update_figure(), no_update


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8050)
