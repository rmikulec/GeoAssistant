import dash
from dash import dcc, html
import plotly.graph_objects as go

import pandas as pd
us_cities = pd.read_csv("https://raw.githubusercontent.com/plotly/datasets/master/us-cities-top-1k.csv")

import plotly.express as px

fig = px.scatter_map(us_cities, lat="lat", lon="lon", hover_name="City", hover_data=["State", "Population"],
                        color_discrete_sequence=["fuchsia"], zoom=3, height=300)
fig.update_layout(
    map_style="dark",
    map_layers=[
        {
            "sourcetype": "vector",
            "sourceattribution": "Locally Hosted PLUTO Dataset",
            "source": [
                "http://localhost:7800/public.parcels/{z}/{x}/{y}.pbf"
            ],
            "sourcelayer": "public.parcels",                   # ← must match your tileset name :contentReference[oaicite:0]{index=0}
            "type": "line",                                 # draw lines
            "color": "#B2FF0C",
            "below": "traces" 
        }
      ]
)
fig.update_layout(margin={"r":0,"t":0,"l":0,"b":0})

app = dash.Dash(__name__)
app.layout = html.Div([
    html.H1("Parcels GeoJSON via pg_tileserv"),
    dcc.Graph(figure=fig, style={"height":"90vh"})
])

if __name__ == "__main__":
    app.run(debug=True)
