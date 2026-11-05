"""
Ingestion de l'entité `flights` (dimension qui évolue souvent : extrait complet quotidien avec
ajouts, changements d'horaire / de date / d'avion, et suppressions).
- bronze : snapshot du jour copié tel quel dans bronze/ ;
- silver : typage (DATE, TIME) + upsert + désactivation des vols disparus (silver_flights, état
  courant) + historisation SCD2 de chaque version (silver_flights_history, bonus) ;
- gold   : dim_flight (état courant) et dim_flight_history (SCD2), avec durée de vol calculée
  (vols de nuit gérés).
"""
from datetime import date

from common import apply_scd2, fetch_csv, get_connection, upsert_snapshot

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
    # Bonus SCD2 : une ligne par VERSION d'un vol, avec sa période de validité [valid_from, valid_to[.
    con.execute("""
        CREATE TABLE IF NOT EXISTS silver_flights_history (
            flight_sk VARCHAR PRIMARY KEY,   -- clé de substitution : <flight_id>@<valid_from>
            flight_id VARCHAR,
            flight_number VARCHAR,
            airline VARCHAR,
            origin_airport_id VARCHAR,
            destination_airport_id VARCHAR,
            flight_date DATE,
            departure_time TIME,
            arrival_time TIME,
            aircraft_type VARCHAR,
            valid_from DATE,
            valid_to DATE,
            is_current BOOLEAN,
            insert_timestamp TIMESTAMP,
            update_timestamp TIMESTAMP
        )
    """)


def ingest_silver(day: date = None, init: bool = False):
    """Relit le snapshot complet des vols depuis bronze/ et le charge en silver.

    Les vols sont une dimension qui évolue souvent (ajouts, changements d'horaire / d'avion /
    de date, suppressions) : upsert + désactivation des vols disparus dans silver_flights, et
    historisation de chaque version dans silver_flights_history.
    """
    subdir, filename = _snapshot_file(day, init)
    df = fetch_csv(subdir, filename)
    snapshot_date = date(2025, 8, 31) if init else day

    con = get_connection()
    create_silver_table(con)
    con.register("snapshot_raw", df)
    # Typage : le bronze est 100 % texte, c'est ici qu'on donne leur vrai type aux colonnes.
    # Les heures sources sont au format "HH:MM" (sans secondes) : format explicite via strptime,
    # un simple CAST(... AS TIME) échoue sur les versions de DuckDB < 1.0.
    con.execute("""
        CREATE OR REPLACE TEMP TABLE snapshot AS
        SELECT
            flight_id,
            flight_number,
            airline,
            origin_airport_id,
            destination_airport_id,
            CAST(flight_date AS DATE)    AS flight_date,
            CAST(strptime(departure_time, '%H:%M') AS TIME) AS departure_time,
            CAST(strptime(arrival_time, '%H:%M') AS TIME)   AS arrival_time,
            aircraft_type
        FROM snapshot_raw
    """)
    upsert_snapshot(con, "silver_flights", "flight_id", FLIGHT_COLS, snapshot_date)
    apply_scd2(con, "silver_flights_history", "flight_sk", "flight_id", FLIGHT_COLS,
               snapshot_date, init)
    con.unregister("snapshot_raw")
    con.close()


# Durée de vol en minutes, modulo 24 h : un vol dont l'heure d'arrivée est "avant" l'heure de
# départ (ex. 15:30 -> 03:41) arrive le lendemain ; sans le modulo la durée serait négative.
_DURATION_SQL = """(
    (hour(arrival_time) * 60 + minute(arrival_time))
    - (hour(departure_time) * 60 + minute(departure_time))
    + 1440) % 1440"""

# Attributs calculés communs à dim_flight et dim_flight_history.
_FLIGHT_GOLD_COLUMNS = f"""
    flight_id,
    flight_number,
    airline,
    origin_airport_id,
    destination_airport_id,
    flight_date,
    departure_time,
    arrival_time,
    aircraft_type,
    {_DURATION_SQL} AS duration_minutes,
    arrival_time < departure_time AS is_overnight,
    flight_date + departure_time AS departure_datetime,
    flight_date + departure_time + to_minutes({_DURATION_SQL}) AS arrival_datetime"""


def ingest_gold():
    """Reconstruit dim_flight (état courant) et dim_flight_history (SCD2) depuis le silver.

    Attributs calculés :
    - duration_minutes : (arrivée - départ) en minutes, modulo 24 h (vols de nuit). Les heures
      sont supposées exprimées dans un même référentiel (pas de fuseaux horaires dans la source :
      limite connue, cf. DOCUMENTATION.md).
    - is_overnight : le vol arrive le jour suivant son départ.
    - departure_datetime / arrival_datetime : horodatages complets, la date d'arrivée étant
      décalée d'un jour pour les vols de nuit.

    Les vols désactivés (is_active = false) sont CONSERVÉS : des réservations passées les
    référencent, fact_booking doit toujours pouvoir les joindre.
    """
    con = get_connection()
    con.execute(f"""
        CREATE OR REPLACE TABLE dim_flight AS
        SELECT
            {_FLIGHT_GOLD_COLUMNS},
            is_active,
            deleted_date,
            insert_timestamp,
            update_timestamp
        FROM silver_flights
    """)
    con.execute(f"""
        CREATE OR REPLACE TABLE dim_flight_history AS
        SELECT
            flight_sk,
            {_FLIGHT_GOLD_COLUMNS},
            valid_from,
            valid_to,
            is_current,
            insert_timestamp,
            update_timestamp
        FROM silver_flights_history
    """)
    con.close()


def init():
    ingest_bronze(init=True)
    ingest_silver(init=True)
    ingest_gold()
    print("Vols (init) ingérés.")


if __name__ == "__main__":
    init()
