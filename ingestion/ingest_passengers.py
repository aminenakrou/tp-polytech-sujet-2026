"""
À compléter : ingestion de l'entité `passengers`, découpée en 3 fonctions bronze/silver/gold
(même structure que `ingest_airports.py`, à utiliser comme modèle).
"""
from datetime import date

from common import fetch_csv, get_connection, upsert_snapshot

# Deux systèmes sources distincts (CRM anglophone / CRM francophone) : on les garde séparés en
# bronze, tels que livrés. La consolidation dans un schéma unique se fera en silver.
SOURCES = ("en", "fr")

PASSENGER_COLS = [
    "first_name", "last_name", "gender", "nationality", "email",
    "birth_date", "signup_date", "source_system",
]


def _snapshot_file(lang: str, day: date = None, init: bool = False):
    if init:
        return "init", f"passengers_{lang}.csv"
    assert day is not None
    return "2025-09", f"passengers_{lang}_{day.isoformat()}.csv"


def ingest_bronze(day: date = None, init: bool = False):
    """Rapatrie les deux snapshots complets (EN et FR) du jour (ou de init/) dans bronze/."""
    for lang in SOURCES:
        subdir, filename = _snapshot_file(lang, day, init)
        fetch_csv(subdir, filename)


def create_silver_table(con):
    # Schéma UNIQUE, en anglais (langue de la majorité des autres sources), quel que soit le CRM
    # d'origine. source_system garde la traçabilité de la provenance de chaque passager.
    con.execute("""
        CREATE TABLE IF NOT EXISTS silver_passengers (
            passenger_id VARCHAR PRIMARY KEY,
            first_name VARCHAR,
            last_name VARCHAR,
            gender VARCHAR,          -- normalisé : 'M' / 'F'
            nationality VARCHAR,
            email VARCHAR,
            birth_date DATE,
            signup_date DATE,
            source_system VARCHAR,   -- 'EN' ou 'FR'
            is_active BOOLEAN,
            deleted_date DATE,
            insert_timestamp TIMESTAMP,
            update_timestamp TIMESTAMP
        )
    """)


def ingest_silver(day: date = None, init: bool = False):
    """Consolide les snapshots EN + FR du jour dans silver_passengers (upsert + désactivation)."""
    df_en = fetch_csv(*_snapshot_file("en", day, init))
    df_fr = fetch_csv(*_snapshot_file("fr", day, init))
    snapshot_date = date(2025, 8, 31) if init else day

    con = get_connection()
    create_silver_table(con)
    con.register("snapshot_en", df_en)
    con.register("snapshot_fr", df_fr)

    # Consolidation des deux CRM : renommage des colonnes FR, harmonisation des valeurs de genre
    # (Male/Female et Homme/Femme -> M/F) et des formats de date (YYYY-MM-DD vs DD/MM/YYYY).
    # Les ids ne se chevauchent pas entre les deux sources (séquence globale P00001...), donc un
    # simple UNION ALL suffit, sans dédoublonnage. La désactivation se fait ensuite sur l'union :
    # un passager n'est désactivé que s'il a disparu de SA source.
    con.execute("""
        CREATE OR REPLACE TEMP TABLE snapshot AS
        SELECT
            passenger_id,
            first_name,
            last_name,
            CASE gender WHEN 'Male' THEN 'M' WHEN 'Female' THEN 'F' END AS gender,
            nationality,
            email,
            CAST(birth_date AS DATE)  AS birth_date,
            CAST(signup_date AS DATE) AS signup_date,
            'EN' AS source_system
        FROM snapshot_en
        UNION ALL
        SELECT
            id_passager AS passenger_id,
            prenom      AS first_name,
            nom         AS last_name,
            CASE genre WHEN 'Homme' THEN 'M' WHEN 'Femme' THEN 'F' END AS gender,
            nationalite AS nationality,
            email,
            CAST(strptime(date_naissance, '%d/%m/%Y') AS DATE)   AS birth_date,
            CAST(strptime(date_inscription, '%d/%m/%Y') AS DATE) AS signup_date,
            'FR' AS source_system
        FROM snapshot_fr
    """)
    upsert_snapshot(con, "silver_passengers", "passenger_id", PASSENGER_COLS, snapshot_date)
    con.unregister("snapshot_en")
    con.unregister("snapshot_fr")
    con.close()


def ingest_gold():
    """Reconstruit dim_passenger à partir de l'état courant de silver_passengers.

    Attributs calculés :
    - age : nombre d'années révolues à la date de référence (date_sub('year', ...) tient compte
      du jour et du mois : on n'a 35 ans que le jour de son anniversaire, pas dès le 1er janvier).
    - age_band : tranche d'âge, bornes classiques du marketing / transport aérien.

    Date de référence = dernier jour de données chargé (max(booking_date) en silver), et non
    current_date : le jeu de données est daté de 2025, un calcul "à aujourd'hui" décalerait tous
    les âges et rendrait le résultat dépendant du jour où l'on lance le pipeline (non
    reproductible). Pour l'âge AU MOMENT de chaque réservation, voir fact_booking.
    """
    con = get_connection()
    con.execute("""
        CREATE OR REPLACE TABLE dim_passenger AS
        WITH ref AS (
            SELECT coalesce(max(booking_date), current_date) AS reference_date FROM silver_bookings
        ),
        aged AS (
            SELECT p.*, ref.reference_date, date_sub('year', p.birth_date, ref.reference_date) AS age
            FROM silver_passengers p CROSS JOIN ref
        )
        SELECT
            passenger_id,
            first_name,
            last_name,
            gender,
            nationality,
            email,
            birth_date,
            signup_date,
            source_system,
            age,
            CASE
                WHEN age < 18 THEN '0-17'
                WHEN age < 25 THEN '18-24'
                WHEN age < 35 THEN '25-34'
                WHEN age < 45 THEN '35-44'
                WHEN age < 55 THEN '45-54'
                WHEN age < 65 THEN '55-64'
                ELSE '65+'
            END AS age_band,
            reference_date AS age_reference_date,
            is_active,
            deleted_date,
            insert_timestamp,
            update_timestamp
        FROM aged
    """)
    con.close()


def init():
    ingest_bronze(init=True)
    ingest_silver(init=True)
    ingest_gold()
    print("Passagers (init) ingérés.")


if __name__ == "__main__":
    init()
