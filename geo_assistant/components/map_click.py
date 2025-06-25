from dash import Dash, html, Input, Output, State, callback_context, no_update
from dash.exceptions import PreventUpdate
import dash_bootstrap_components as dbc
from geo_assistant.components import css


class DataView(html.Div):
    """
    A frosted-glass container for grouped data views.
    """
    _base_style = {
        **css.FROSTED_CONTAINER,
        "padding": "1rem",
        "marginBottom": "1rem",
    }

    def __init__(self, children: list, **kwargs):
        style = {**self._base_style, **kwargs.pop('style', {})}
        super().__init__(children=children, style=style, **kwargs)


class KeyValueTable(html.Table):
    """
    A two-column table for key → value display, styled for readability.
    """
    _table_style = {
        "width": "100%",
        "margin": 0,
        "borderSpacing": "0 0.25rem",
        "borderCollapse": "separate",
    }
    _pad = {"padding": "0.25rem 0.5rem"}
    _key_style = {"fontWeight": "bold", "color": "#fff", **_pad}
    _value_style = {"color": "#fff", **_pad}

    def __init__(self, data: dict, **kwargs):
        header = html.Thead(
            html.Tr([
                html.Th("Property", style={**self._key_style, "borderBottom": "1px solid rgba(255,255,255,0.3)"}),
                html.Th("Value", style={**self._value_style, "borderBottom": "1px solid rgba(255,255,255,0.3)"}),
            ]),
            style={
                "position": "sticky",
                "top": 0,
                "backgroundColor": "rgba(255,255,255,0.1)",
                "zIndex": 1,
            }
        )
        body_rows = []
        for k, v in data.items():
            body_rows.append(
                html.Tr([
                    html.Td(str(k), style=self._key_style),
                    html.Td(str(v), style=self._value_style),
                ])
            )
        super().__init__(
            [header, html.Tbody(body_rows)],
            style={**self._table_style, **kwargs.pop('style', {})},
            **kwargs,
        )


class FeatureView(html.Div):
    """
    Renders one or more feature tables in sequence.
    """
    def __init__(self, features: list, **kwargs):
        items = []
        for idx, feat in enumerate(features, start=1):
            if len(features) > 1:
                items.append(html.H6(f"Feature {idx}", className="mt-2 text-white"))
            items.append(KeyValueTable(feat))
        super().__init__(children=items, **kwargs)


class MapClickModal(dbc.Modal):
    """
    A modal that pops up on map clicks, styled like the chat drawer.
    """
    DEFAULT_ID = "coords-modal"
    BODY_ID = "coords-modal-body"
    CLOSE_ID = "coords-modal-close"
    LISTENER_ID = "map-listener"

    def __init__(self, id: str = DEFAULT_ID, **kwargs):
        # Header, body, footer containers
        header = dbc.ModalHeader(
            html.H5("Map Click Info", className="m-0 text-white")
        )
        body = dbc.ModalBody(id=self.BODY_ID)
        footer = dbc.ModalFooter(
            dbc.Button(
                "Close",
                id=self.CLOSE_ID,
                n_clicks=0,
                className="text-white",
                style={
                    **css.FROSTED_INPUT,
                    "border": "1px solid rgba(255,255,255,0.6)",
                },
            ),
            className="justify-content-end"
        )

        super().__init__(
            [header, body, footer],
            id=id,
            is_open=False,
            size="lg",
            centered=True,
            scrollable=False,
            backdrop=True,
            fade=True,
            # Match chat drawer styling
            content_style={
                **css.FROSTED_CONTAINER,
                "border": "1px solid rgba(255,255,255,0.6)",
                "borderRadius": "0.5rem",
                "padding": "1rem",
                "maxWidth": "80vw",
            },
            backdrop_style={"backgroundColor": "rgba(0,0,0,0.6)"},
            backdropClassName="modal-backdrop",
            **kwargs,
        )

    @classmethod
    def register_callbacks(cls, app: Dash, listener_id: str = LISTENER_ID) -> None:
        """
        Registers the callbacks to a DashApp with a map event listener. This component will recieve
        data from the MapLatLong event listener to display to the user

        Args:
            - app (Dash): The dash application holding the component. MUST have a Websocket
                component as well
            - listener_id (str): The ID of the EventListener Component
        """
        @app.callback(
            Output(cls.DEFAULT_ID, "is_open"),
            Output(cls.BODY_ID, "children"),
            Input(listener_id, "n_events"),
            Input(cls.CLOSE_ID, "n_clicks"),
            State(listener_id, "event"),
            State(cls.DEFAULT_ID, "is_open"),
            prevent_initial_call=True,
        )
        def _toggle(n_map, n_close, event, is_open):
            """
            Callback to toggle the model. This loads in all the data when a spot on the map is
                clicked, and closes it if exit is triggered
            """
            ctx = callback_context
            if not ctx.triggered:
                raise PreventUpdate
            trg = ctx.triggered[0]["prop_id"].split('.')[0]

            # Create and open the model if callback was trigger by Map Event Listener
            if trg == listener_id:
                lat = event["detail.lat"]
                lon = event["detail.lon"]
                x = event["detail.x"]
                y = event["detail.y"]
                results = event.get("detail.results") or []

                coord_items = [
                    html.Div(f"📍 Lat: {lat:.5f}   Lon: {lon:.5f}", className="text-white mb-1"),
                    html.Div(f"🖱️  X: {x:.1f}        Y: {y:.1f}", className="text-white"),
                ]
                coord_view = DataView(coord_items)

                if results:
                    feat_header = html.H6("Features under cursor:", className="text-white mb-2")
                    feat_table = FeatureView(results)
                    feat_pane = DataView(
                        [feat_header, feat_table],
                        style={"maxHeight": "60vh", "overflowY": "auto"},
                    )
                else:
                    feat_pane = DataView([
                        html.Div("No features at this location.", className="text-white")
                    ])

                row = dbc.Row([
                    dbc.Col(coord_view, width=4),
                    dbc.Col(feat_pane, width=8)
                ], className="g-3")

                return True, [row]

            # Close it if it was triggered by a close button
            if trg == cls.CLOSE_ID:
                return False, no_update

            return is_open, no_update

