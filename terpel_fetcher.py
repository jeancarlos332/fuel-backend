import os
import re
import requests  # type: ignore
import psycopg2  # type: ignore
from datetime import datetime
from psycopg2.extras import Json # type: ignore
from datetime import datetime, timezone

TERPEL_URL = "https://www.terpel.com/api/map_points/eds"

DB_CONFIG = {
    "dbname": os.getenv("PGDATABASE"),
    "user": os.getenv("PGUSER"),
    "password": os.getenv("PGPASSWORD"),
    "host": os.getenv("PGHOST"),
    "port": os.getenv("PGPORT", 5432),
}


def fetch_terpel():
    headers = {
        "User-Agent": "Mozilla/5.0",
        "Accept": "application/json"
    }

    res = requests.get(TERPEL_URL, headers=headers, timeout=20)
    res.raise_for_status()
    return res.json()


def make_station_hash(nom, lat, lon):
    base = f"{nom}-{lat}-{lon}".lower()
    base = re.sub(r"[^a-z0-9]+", "_", base)
    return base.strip("_")

def normalize_prices(price_list):
    price_map = {}

    for p in price_list or []:
        product = p["productName"].lower()

        if "corriente" in product:
            key = "corriente"
        elif "acpm" in product or "diesel" in product:
            key = "acpm"
        else:
            key = product.replace(" ", "_")

        price_map[key] = p["retailPrice"]

    return price_map

def normalize_terpel(st):
    station_uid = make_station_hash(st["nom"], st["lat"], st["lon"])

    prices = normalize_prices(st.get("price", []))

    return {
        "id": f"terpel_{station_uid}",
        "brand": "Terpel",
        "nombre": st.get("nom"),
        "direccion": st.get("dir"),
        "ciudad": st.get("ciu"),
        "departamento": st.get("dep"),
        "pais": st.get("pai"),
        "lat": float(st["lat"]),
        "lng": float(st["lon"]),
        "precio": prices,            
        "fuel_type": "multi",       
        "services": [s["name"] for s in st.get("services", []) if s.get("name")],
        "programs": [p["name"] for p in st.get("programs", []) if p.get("name")],
        "source": "terpel",
        "fetched_at": datetime.now(timezone.utc)
    }


def save_to_db(stations):
    conn = psycopg2.connect(**DB_CONFIG)

    try:
        cur = conn.cursor()

        for i, s in enumerate(stations):

            try:
                s["precio"] = Json(s["precio"])
                s["services"] = Json(s["services"])
                s["programs"] = Json(s["programs"])

                cur.execute("""
                INSERT INTO estaciones
                (id, brand, nombre, direccion, ciudad, departamento, pais,
                 precio, fuel_type, services, programs,
                 geom, source, updated_at)
                VALUES (
                    %(id)s,
                    %(brand)s,
                    %(nombre)s,
                    %(direccion)s,
                    %(ciudad)s,
                    %(departamento)s,
                    %(pais)s,
                    %(precio)s,
                    %(fuel_type)s,
                    %(services)s,
                    %(programs)s,
                    %(lat)s,
                    %(lng)s,
                    %(source)s,
                    %(fetched_at)s
                )
                ON CONFLICT (id) DO UPDATE
                SET 
                    precio = EXCLUDED.precio,
                    services = EXCLUDED.services,
                    programs = EXCLUDED.programs,
                    updated_at = EXCLUDED.updated_at;
                """, s)

            except Exception as e:
                print(f"Error guardando estación: {s.get('nombre')} → {e}")
                conn.rollback() 
                continue 

       
            if (i + 1) % 50 == 0:
                conn.commit()

        conn.commit() 

    finally:
        cur.close()
        conn.close()


if __name__ == "__main__":
    raw = fetch_terpel()

    normalized = []
    for s in raw:
        try:
            normalized.append(normalize_terpel(s))
        except Exception as e:
            print(f"Error normalizando estación {s.get('nom')}: {e}")

    save_to_db(normalized)
    print(f"Actualizadas {len(normalized)} estaciones Terpel")
