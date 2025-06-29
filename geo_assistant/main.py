# main.py
from dotenv import load_dotenv
load_dotenv()


import json
import uvicorn
from sqlalchemy import create_engine
from fastapi import FastAPI, WebSocket, WebSocketDisconnect, HTTPException
from fastapi.responses import JSONResponse
from starlette.middleware.cors import CORSMiddleware

from geo_assistant import GeoAgent
from geo_assistant.handlers import PlotlyMapHandler, PostGISHandler
from geo_assistant.config import Configuration
from geo_assistant.logging import get_logger
from geo_assistant.agent.updates import EmitUpdate

logger = get_logger(__name__)


# 1) Create the FastAPI app
app = FastAPI()
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://127.0.0.1:8200"],  # or ["*"] 
    allow_credentials=True,
    allow_methods=["GET", "POST", "PUT", "DELETE", "OPTIONS"],
    allow_headers=["*"],
)

# 2) Initialize your GeoAgent (or other shared state)
engine = create_engine(Configuration.db_connection_url)
agent = GeoAgent(
    engine=engine,
    map_handler=PlotlyMapHandler(),
    data_handler=PostGISHandler(),
)

# 3) Mount the Dash app under /dash
#dash_app = create_dash_app(app, agent.map_handler.figure)
#app.mount("/dash", WSGIMiddleware(dash_app.server))
# (Dash’s own WSGI/AWSGI app is dash_app.server, and FastAPI is ASGI,
#  so behind the scenes Starlette wraps that for you.)

# 4) Define your WebSocket endpoint alongside any future REST routes
@app.websocket("/ws")
async def websocket_endpoint(ws: WebSocket):
    await ws.accept()

    async def _websocket_emit(udpate: EmitUpdate):
        await ws.send_text(
            udpate.model_dump_json()
        )

    agent.emitter = _websocket_emit
    try:
        while True:
            raw = await ws.receive_text()
            data = json.loads(raw)
            logger.info(f"Message recieved: {raw}")

            # only handle user messages
            if data.get("type") != "user":
                continue

            user_message = data["message"]
            await ws.send_text(json.dumps({'type': "user_message", "message": user_message}))

            # stream back all of your events—
            # chat_stream should be an async generator
            await agent.chat(user_message)

    except WebSocketDisconnect:
        print("Client disconnected")

# New endpoint to serve the map figure
@app.get("/map-figure")
async def get_map_figure():
    # Grab the figure from your singleton agent
    fig = agent.map_handler.figure
    # Convert to Plotly JSON
    return JSONResponse(content=fig.to_plotly_json())

@app.get("/query/lat-long/{lat}/{lon}")
def query_lat_long(
    lat: float,
    lon: float
):
    """
    Query the given table for features within a small distance of (lat, long).
    Delegates to agent.map_handler under the hood.
    """
    try:
        # map_handler should return whatever JSON-able object you want to send back
        return agent.data_handler.get_latlong_data(
            engine=engine,
            lat=lat,
            lon=lon,
        )
    except Exception as e:
        # wrap any errors in an HTTPException so FastAPI can return a proper 4xx/5xx
        raise HTTPException(status_code=404, detail=e)

# 5) Run it all together
if __name__ == "__main__":
    try:
        uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)
    finally:
        agent.registry.cleanup(agent.engine)
