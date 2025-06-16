import dash
from flask import Response, request, g
import psycopg

# ——— 1) Create Dash + grab Flask server ———
app = dash.Dash(__name__)
server = app.server

# ——— 2) PostGIS connection (sync example) ———

def get_db():
    # lazily create and stash in flask.g
    if "db" not in g:
        g.db = psycopg.connect(
        dbname="yourdb",
        user="you",
        password="secret",
        host="127.0.0.1",
        port=5432
    )
    return g.db

@server.teardown_appcontext
def close_db(exc):
    # on every request teardown, close the connection if it exists
    db = g.pop("db", None)
    if db is not None:
        db.close()


def tile_bounds(z, x, y):
    size = 40075016.68557849
    tile = size / (2**z)
    xmin = x * tile - size/2
    xmax = (x+1)*tile - size/2
    ymax = size/2 - y*tile
    ymin = size/2 - (y+1)*tile
    return xmin, ymin, xmax, ymax

@server.route('/tiles/<int:z>/<int:x>/<int:y>.pbf')
def tile(z, x, y):
    conn = get_db()
    cur = conn.cursor()
    # read any filter params you passed in the URL
    borough = request.args.get("borough")        # e.g. “Manhattan”
    min_lot_area = request.args.get("minArea", type=float)
    
    xmin, ymin, xmax, ymax = tile_bounds(z, x, y)
    
    filters = ["geom_3857 && ST_MakeEnvelope(%s,%s,%s,%s,3857)"]
    params = [xmin, ymin, xmax, ymax]
    if borough:
        filters.append("borough = %s")
        params.append(borough)
    if min_lot_area is not None:
        filters.append("lot_area >= %s")
        params.append(min_lot_area)
    where_sql = " AND ".join(filters)
    
    sql = f"""
    WITH
      tile AS (
        SELECT
          ST_AsMVTGeom(
            geom_3857,
            ST_MakeEnvelope(%s,%s,%s,%s,3857),
            4096, 256, TRUE
          ) AS geom_clip,
          borough, lot_area
        FROM parcels
        WHERE {where_sql}
      )
    SELECT ST_AsMVT(tile.*, 'parcels') AS mvt FROM tile;
    """
    # Note: ST_AsMVTGeom needs the envelope params again, so we pass params*2
    cur.execute(sql, params)
    blob = cur.fetchone()[0]
    return Response(blob, mimetype="application/x-protobuf")


if __name__ == '__main__':
    app.run(debug=True)