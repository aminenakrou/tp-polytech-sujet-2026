"""
À compléter : ingestion de l'entité `bookings`, découpée en 3 fonctions bronze/silver/gold
(même structure que `ingest_airports.py`, à utiliser comme modèle).
"""
from datetime import date

from common import fetch_csv, get_connection


def _daily_file(day: date = None, init: bool = False):
    # Contrairement aux 3 autres entités, ce n'est pas un snapshot complet : chaque fichier ne
    # contient que les réservations effectuées ce jour-là (init/ = tout le mois d'août).
    if init:
        return "init", "bookings.csv"
    assert day is not None
    return "2025-09", f"bookings_{day.isoformat()}.csv"


def ingest_bronze(day: date = None, init: bool = False):
    """Rapatrie le fichier des réservations du jour (ou de init/) dans bronze/."""
    subdir, filename = _daily_file(day, init)
    fetch_csv(subdir, filename)


def create_silver_table(con):
    # Pas de is_active / deleted_date : une réservation est un évènement immuable, elle ne
    # disparaît pas et ne change pas. update_timestamp est gardé par convention (toutes les tables
    # l'ont) : il vaut insert_timestamp tant que la ligne n'est jamais retouchée.
    con.execute("""
        CREATE TABLE IF NOT EXISTS silver_bookings (
            booking_id VARCHAR PRIMARY KEY,
            passenger_id VARCHAR,
            flight_id VARCHAR,
            airport_id VARCHAR,
            seat_class VARCHAR,
            amount DECIMAL(12, 2),
            currency VARCHAR,
            booking_date DATE,
            booking_channel VARCHAR,
            insert_timestamp TIMESTAMP,
            update_timestamp TIMESTAMP
        )
    """)


def ingest_silver(day: date = None, init: bool = False):
    """Charge les réservations du jour dans silver_bookings en INSERT simple (idempotent).

    Le fichier ne contient que les nouveautés du jour et une réservation ne change jamais :
    un upsert n'aurait aucun sens. ON CONFLICT DO NOTHING rend le chargement rejouable : relancer
    le même jour ne crée aucun doublon. Surtout pas de désactivation ici : une réservation absente
    du fichier du jour n'a pas disparu, elle a simplement été faite un autre jour.
    """
    subdir, filename = _daily_file(day, init)
    df = fetch_csv(subdir, filename)

    con = get_connection()
    create_silver_table(con)
    con.register("daily_raw", df)
    con.execute("""
        INSERT INTO silver_bookings
        SELECT
            booking_id,
            passenger_id,
            flight_id,
            airport_id,
            seat_class,
            CAST(amount AS DECIMAL(12, 2)) AS amount,
            currency,
            CAST(booking_date AS DATE) AS booking_date,
            booking_channel,
            now() AS insert_timestamp,
            now() AS update_timestamp
        FROM daily_raw
        ON CONFLICT (booking_id) DO NOTHING
    """)

    # Contrôle qualité : chaque réservation doit référencer un passager, un vol et un aéroport
    # connus en silver (run_month.py charge le silver des dimensions AVANT celui des bookings).
    # On ne bloque pas le chargement (la réservation a bien eu lieu), on signale l'anomalie.
    orphans = con.execute("""
        SELECT count(*) FROM daily_raw b
        WHERE NOT EXISTS (SELECT 1 FROM silver_passengers p WHERE p.passenger_id = b.passenger_id)
           OR NOT EXISTS (SELECT 1 FROM silver_flights f WHERE f.flight_id = b.flight_id)
           OR NOT EXISTS (SELECT 1 FROM silver_airports a WHERE a.airport_id = b.airport_id)
    """).fetchall()[0][0]  # fetchall : DuckDB 2 exige de consommer le résultat avant unregister
    if orphans:
        print(f"  [WARN] bookings {filename} : {orphans} réservation(s) avec une référence inconnue")

    con.unregister("daily_raw")
    con.close()


def ingest_gold():
    # TODO : reconstruire la/les table(s) de gold avec les données booking à partir de silver_bookings. 
    raise NotImplementedError


def init():
    ingest_bronze(init=True)
    ingest_silver(init=True)
    ingest_gold()
    print("Réservations (init) ingérés.")


if __name__ == "__main__":
    init()
