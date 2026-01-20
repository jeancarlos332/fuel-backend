import os
from fastapi import FastAPI, Query
import psycopg2
from psycopg2.extras import RealDictCursor
from math import radians, sin, cos, sqrt, atan2
from dotenv import load_dotenv
from fastapi.middleware.cors import CORSMiddleware

load_dotenv()
app = FastAPI(title="Fuel Prices API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


DB_CONFIG = {
    "dbname": os.getenv("PGDATABASE"),
    "user": os.getenv("PGUSER"),
    "password": os.getenv("PGPASSWORD"),
    "host": os.getenv("PGHOST"),
    "port": int(os.getenv("PGPORT", 5432)),
}

def get_db():
    return psycopg2.connect(
        dbname=os.getenv("PGDATABASE"),
        user=os.getenv("PGUSER"),
        password=os.getenv("PGPASSWORD"),
        host=os.getenv("PGHOST"),
        port=os.getenv("PGPORT", 5432),
        cursor_factory=RealDictCursor,
    )

def haversine(lat1, lon1, lat2, lon2):
    R = 6371
    dlat = radians(lat2 - lat1)
    dlon = radians(lon2 - lon1)
    a = sin(dlat / 2) ** 2 + cos(radians(lat1)) * cos(radians(lat2)) * sin(dlon / 2) ** 2
    c = 2 * atan2(sqrt(a), sqrt(1 - a))
    return R * c

def normalize_price_filter(precio_json, fuel_type):
    try:
        if isinstance(precio_json, str):
            precio_dict = json.loads(precio_json)
        else:
            precio_dict = precio_json

        key = fuel_type.lower()

        return precio_dict.get(key)

    except Exception as e:
        print("⚠️ Error procesando precio:", e)
        return None


@app.get("/stations/nearby")
def get_nearby_stations(
    lat: float = Query(...),
    lng: float = Query(...),
    radius_km: float = Query(5),
    fuel_type: str = Query("Corriente"),
):
    conn = get_db()
    cur = conn.cursor()

    cur.execute("""
        SELECT 
            id, brand, nombre, direccion, precio, 
            lat, lng, updated_at, ciudad, departamento
        FROM estaciones;
    """)

    rows = cur.fetchall()
    cur.close()
    conn.close()

    results = []

    for row in rows:
        dist = haversine(lat, lng, row["lat"], row["lng"])

        if dist <= radius_km:
            precio_filtrado = normalize_price_filter(row["precio"], fuel_type)

            results.append({
                "id": row["id"],
                "brand": row["brand"],
                "nombre": row["nombre"],
                "direccion": row["direccion"],
                "ciudad": row["ciudad"],
                "departamento": row["departamento"],
                "lat": row["lat"],
                "lng": row["lng"],
                "dist_km": round(dist, 2),
                "precio": precio_filtrado,
                "updated_at": row["updated_at"],
            })

    results = sorted(results, key=lambda x: x["dist_km"])

    top5 = results[:5]

    top5 = sorted(
        top5,
        key=lambda x: (x["precio"] is None, x["precio"] if x["precio"] is not None else 999999999)
    )

    return {
        "count": len(top5),
        "fuel_type": fuel_type,
        "radius_km": radius_km,
        "stations": top5,
    }
