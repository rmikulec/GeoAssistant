import dash
from geo_assistant.logging import get_logger
import asyncio
from sqlalchemy import create_engine
from dash import html, dcc, Input, Output, State, no_update
import dash_bootstrap_components as dbc

from geo_assistant.handlers import MapHandler, DataHandler
from geo_assistant.agent._agent import GeoAgent
from geo_assistant.config import Configuration


# Initialize Classes
logger = get_logger(__name__)
# Set up app
engine = create_engine(url=Configuration.db_connection_url)
app = dash.Dash(__name__, external_stylesheets=[dbc.themes.BOOTSTRAP, dbc.icons.FONT_AWESOME])
server = app.server


# Set up geo-assistant
agent = GeoAgent(
    engine=engine,
    map_handler=MapHandler(),
    data_handler=DataHandler(
        default_table="pluto"
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
                type="default",
                color="rgba(255, 255, 255, 0.8)"
            ),
            id="open-chat", color="primary", size="lg", className="glass-button"
        ),
        style={"position": "fixed", "top": "15px", "right": "15px", "zIndex": 1000},
    ),

    dbc.Offcanvas(
        html.Div([
            html.H5("Chat", className="mb-1 text-white"),

            # apply our frosted-glass chat log class
            html.Div(
                id="chat-log",
                className="flex-grow-1 overflow-auto mb-3 glass-chat-log",
                style={"padding": "5px", "whiteSpace": "pre-wrap", "color": "#fff"}
            ),

            # input + send button group
            dbc.InputGroup(
                [
                    dbc.Input(
                        id="chat-input",
                        placeholder="Type your message…",
                        type="text",
                        debounce=True,
                        className="glass-input text-white"
                    ),
                    dbc.Button(
                        html.I(className="fa-solid fa-paper-plane"),
                        id="send-btn",
                        color="light",
                        n_clicks=0,
                        className="glass-button"
                    ),
                ],
                className="mt-auto",
            ),
        ],
        style={"height": "100%", "display": "flex", "flexDirection": "column"}
        ),
        id="chat-drawer",
        is_open=False,
        placement="end",
        className="glass-offcanvas",
        style={"width": "400px", "zIndex": "1100"},
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
        return existing_log, "", no_update, no_update
    
    # start from empty list if None
    log = existing_log or []
    
    # wrap each message in a div (you can customize the className/style)
    # user bubble
    log.append(
        html.Div(
            new_message,
            className="chat-message user-message"
        )
    )

    ai_response = asyncio.run(agent.chat(new_message))

    # assistant bubble
    log.append(
        html.Div(
            ai_response,
            className="chat-message assistant-message"
        )
    )
    
    # clear the input after sending
    return log, "", agent.map_handler.update_figure(), no_update


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8050)
