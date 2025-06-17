import dash
import logging
import pathlib
import json
from dash import html, dcc, Input, Output, State
import dash_bootstrap_components as dbc

import plotly.graph_objects as go
import plotly.express as px


from geo_assistant.vector_store import FieldDefinitionStore
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
        db_name="parcelsdb",
        table_name="parcels"
    )
)
# Setup vector store
pdf_path = pathlib.Path("./pluto/pluto_datadictionary.pdf")
export_path = pathlib.Path("./pluto/field_def_index")
if export_path.exists():
    index = FieldDefinitionStore.load(export_path)
else:
    index = FieldDefinitionStore.from_pdf(pdf_path, export_path)



app.layout = html.Div([
    # full-screen graph
    dcc.Graph(
        id="map-graph",
        figure=agent.map_handler.figure,
        style={"position": "absolute", "top": 0, "left": 0, "right": 0, "bottom": 0},
        config={
        'displayModeBar': False
    }
    ),

    # fixed container for the button
    html.Div(
        dbc.Button(html.I(className='fa-solid fa-comments'), id="open-chat", color="primary", size="lg"),
        style={
            "position": "fixed",
            "top": "10px",
            "right": "10px",
            "zIndex": 1000,
        }
    ),

    # the off-canvas sidebar
dbc.Offcanvas(
    # wrap everything in a flex‐column container that fills the height
    html.Div(
        [
            # Header
            html.H5("Chat", className="mb-3"),

            # Chat log area (scrollable)
            html.Div(
                id="chat-log",
                className="flex-grow-1 overflow-auto mb-3",
                style={
                    "backgroundColor": "#f8f9fa",
                    "padding": "10px",
                    "borderRadius": "4px",
                    "border": "1px solid #dee2e6",
                },
            ),

            # Input area pinned to bottom
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
                className="mt-auto",  # pushes this group to the bottom
            ),
        ],
        style={
            "height": "100%",             # fill Offcanvas vertically
            "display": "flex",
            "flexDirection": "column",
        },
    ),
    id="chat-drawer",
    title="",            # title moved into header H5
    is_open=False,
    placement="end",
    style={"width": "350px"},
)

])

@app.callback(
    Output("chat-drawer", "is_open"),
    Input("open-chat", "n_clicks"),
    State("chat-drawer", "is_open")
)
def toggle_chat(n, is_open):
    print("opening")
    if n:
        return not is_open
    return is_open


@app.callback(
    # 1) Append to the chat log
    Output("chat-log", "children"),
    # 2) Clear the input field
    Output("chat-input", "value"),
    # 3) The new figure for the graph
    Output("map-graph", "figure"),
    Input("send-btn", "n_clicks"),
    State("chat-input", "value"),
    State("chat-log", "children"),
    prevent_initial_call=True,
)
def send_message(n_clicks, new_message, existing_log):
    if not new_message:
        # no change if input is empty
        return existing_log, ""
    
    # start from empty list if None
    log = existing_log or []
    
    # wrap each message in a div (you can customize the className/style)
    log.append(html.Div(f"User: {new_message}", className="mb-2"))
    field_def_query = new_message
    if len(agent.messages)>1:
        field_def_query += " " + agent.messages[-1]['content']
    field_defs = index.query(new_message, k=10)
    ai_response = agent.chat(new_message, field_defs)
    log.append(html.Div(f"GeoAssistant: {ai_response}", className="mb-2"))
    
    # clear the input after sending
    return log, "", agent.map_handler.update_figure()


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8050)
