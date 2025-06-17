import dash
from dash import html, dcc, Input, Output, State
import dash_bootstrap_components as dbc

import plotly.express as px

fig = px.choropleth_map(height=300, zoom=3)
fig.update_layout(
    map_style="dark",
    map_layers=[
        {
            "sourcetype": "vector",
            "sourceattribution": "Locally Hosted PLUTO Dataset",
            "source": [
                "http://localhost:7800/public.parcels/{z}/{x}/{y}.pbf?columns%20%3D%20%27BBL%27&filter=Borough%20%3D%20%27BK%27"
            ],
            "sourcelayer": "public.parcels",                   # ← must match your tileset name :contentReference[oaicite:0]{index=0}
            "type": "line",                                 # draw lines
            "color": "#B2FF0C",
            "below": "traces" 
        },
        {
            "sourcetype": "vector",
            "sourceattribution": "Locally Hosted PLUTO Dataset",
            "source": [
                "http://localhost:7800/public.parcels/{z}/{x}/{y}.pbf?columns%20%3D%20%27BBL%27&filter=Borough%20%3D%20%27QN%27"
            ],
            "sourcelayer": "public.parcels",                   # ← must match your tileset name :contentReference[oaicite:0]{index=0}
            "type": "line",                                 # draw lines
            "color": "#7803FF",
            "below": "traces" 
        }
      ]
)
fig.update_layout(margin={"r":0,"t":0,"l":0,"b":0})

fig.update_layout(map_bounds={
    "west":-74.2562146223856,
    "east":-73.70017542782129,
    "north": 40.91631520032119,
    "south": 40.49449291374346
})

app = dash.Dash(__name__, external_stylesheets=[dbc.themes.BOOTSTRAP, dbc.icons.FONT_AWESOME])
app.layout = html.Div([
    # full-screen graph
    dcc.Graph(
        id="map-graph",
        figure=fig,
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
    if n:
        return not is_open
    return is_open

if __name__ == "__main__":
    app.run(debug=True)
