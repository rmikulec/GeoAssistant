# main.py
import json
import uvicorn
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import JSONResponse
from starlette.middleware.wsgi import WSGIMiddleware
from geo_assistant.dash_app import create_dash_app
from geo_assistant.agent._agent import GeoAgent
from geo_assistant.handlers import PlotlyMapHandler, PostGISHandler, DashLeafletMapHandler
from sqlalchemy import create_engine
from geo_assistant.config import Configuration
from geo_assistant.logging import get_logger

logger = get_logger(__name__)


# 1) Create the FastAPI app
app = FastAPI()

# 2) Initialize your GeoAgent (or other shared state)
engine = create_engine(Configuration.db_connection_url)
agent = GeoAgent(
    engine=engine,
    map_handler=DashLeafletMapHandler(),
    data_handler=PostGISHandler(default_table="pluto"),
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
    agent._set_websocket(ws)
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
    fig = agent.map_handler.update_figure()
    # Convert to Plotly JSON
    return JSONResponse(content=fig)

# 5) Run it all together
if __name__ == "__main__":
    try:
        uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)
    finally:
        agent.registry.cleanup(agent.engine)
