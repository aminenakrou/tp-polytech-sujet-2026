"""
À compléter : ingestion de l'entité `flights`, découpée en 3 fonctions bronze/silver/gold
(même structure que `ingest_airports.py`, à utiliser comme modèle).
"""
from datetime import date

from common import fetch_csv, get_connection, upsert_snapshot

FLIGHT_COLS = [
    "flight_number", "airline", "origin_airport_id", "destination_airport_id",
    "flight_date", "departure_time", "arrival_time", "aircraft_type",
]


def _snapshot_file(day: date = None, init: bool = False):
    if init:
        return "init", "flights.csv"
    assert day is not None
    return "2025-09", f"flights_{day.isoformat()}.csv"


def ingest_bronze(day: date = None, init: bool = False):
    """Rapatrie le snapshot complet des vols du jour (ou de init/) dans bronze/."""
    subdir, filename = _snapshot_file(day, init)
    fetch_csv(subdir, filename)


def create_silver_table(con):
    con.execute("""
        CREATE TABLE IF NOT EXISTS silver_flights (
            flight_id VARCHAR PRIMARY KEY,
            flight_number VARCHAR,
            airline VARCHAR,
            origin_airport_id VARCHAR,
            destination_airport_id VARCHAR,
            flight_date DATE,
            departure_time TIME,
            arrival_time TIME,
            aircraft_type VARCHAR,
            is_active BOOLEAN,
            deleted_date DATE,
            insert_timestamp TIMESTAMP,
            update_timestamp TIMESTAMP
        )
    """)


def ingest_silver(day: date = None, init: bool = False):
    """Relit le snapshot complet des vols depuis bronze/ et l'upsert dans silver_flights.

    Les vols sont une dimension qui évolue souvent (ajouts, changements d'horaire / d'avion /
    de date, suppressions) : upsert + désactivation des vols disparus.
    """
    subdir, filename = _snapshot_file(day, init)
    df = fetch_csv(subdir, filename)
    snapshot_date = date(2025, 8, 31) if init else day

    con = get_connection()
    create_silver_table(con)
    con.register("snapshot_raw", df)
    # Typage : le bronze est 100 % texte, c'est ici qu'on donne leur vrai type aux colonnes.
    con.execute("""
        CREATE OR REPLACE TEMP TABLE snapshot AS
        SELECT
            flight_id,
            flight_number,
            airline,
            origin_airport_id,
            destination_airport_id,
            CAST(flight_date AS DATE)    AS flight_date,
            CAST(departure_time AS TIME) AS departure_time,
            CAST(arrival_time AS TIME)   AS arrival_time,
            aircraft_type
        FROM snapshot_raw
    """)
    upsert_snapshot(con, "silver_flights", "flight_id", FLIGHT_COLS, snapshot_date)
    con.unregister("snapshot_raw")
    con.close()


def ingest_gold():
    # TODO : reconstruire la/les table(s) de gold avec les données flights à partir de silver_flights
    raise NotImplementedError


def init():
    ingest_bronze(init=True)
    ingest_silver(init=True)
    ingest_gold()
    print("Vols (init) ingérés.")


if __name__ == "__main__":
    init()
