"""
Ingestion de l'entité `bookings`, la table de FAIT : évènements immuables, chaque fichier ne
contient que les réservations du jour.
- bronze : fichier du jour copié tel quel dans bronze/ ;
- silver : insert simple idempotent (ON CONFLICT DO NOTHING), aucune désactivation ;
- gold   : dim_date, fact_booking (amount_eur) et la table d'agrégation
  agg_daily_revenue_airline (chiffre d'affaires par jour et par compagnie).
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
    """Construit la partie "faits" du gold : dim_date, fact_booking et agg_daily_revenue_airline.

    Doit tourner APRÈS le gold des vols et des passagers (garanti par l'ordre de run_month.py) :
    l'agrégat par compagnie a besoin de dim_flight à jour.
    """
    con = get_connection()

    # --- dim_date : calendrier couvrant les dates de réservation ET de vol (les vols réservés
    # en septembre peuvent partir en octobre). Chargée en insert idempotent plutôt que
    # reconstruite : une date ne change jamais, son insert_timestamp reste donc stable.
    con.execute("""
        CREATE TABLE IF NOT EXISTS dim_date (
            date_key DATE PRIMARY KEY,
            year INTEGER,
            month INTEGER,
            day INTEGER,
            day_of_week INTEGER,     -- ISO : 1 = lundi ... 7 = dimanche
            day_name VARCHAR,
            is_weekend BOOLEAN,
            insert_timestamp TIMESTAMP,
            update_timestamp TIMESTAMP
        )
    """)
    con.execute("""
        INSERT INTO dim_date
        WITH bounds AS (
            SELECT
                least((SELECT min(booking_date) FROM silver_bookings),
                      (SELECT min(flight_date) FROM silver_flights))    AS d_min,
                greatest((SELECT max(booking_date) FROM silver_bookings),
                         (SELECT max(flight_date) FROM silver_flights)) AS d_max
        ),
        -- generate_series en fonction scalaire (liste) + unnest : syntaxe compatible avec toutes
        -- les versions de DuckDB autorisées par requirements.txt (>= 0.10).
        days AS (
            SELECT CAST(unnest(generate_series(
                CAST(d_min AS TIMESTAMP), CAST(d_max AS TIMESTAMP), INTERVAL 1 DAY
            )) AS DATE) AS d
            FROM bounds
        )
        SELECT
            d,
            year(d), month(d), day(d),
            isodow(d),
            dayname(d),
            isodow(d) >= 6,
            now(), now()
        FROM days
        ON CONFLICT (date_key) DO NOTHING
    """)

    # --- fact_booking : une ligne par réservation (grain = la réservation).
    # amount_eur = montant normalisé en euros via dim_currency (taux fixes fournis).
    # passenger_age_at_booking : l'âge au moment de la réservation, figé (contrairement à
    # dim_passenger.age qui dépend de la date de référence).
    # days_before_departure : délai de réservation, mesure classique du transport aérien.
    # Jointures LEFT : une réservation n'est jamais perdue même si une référence manquait.
    # Bonus SCD2 : flight_sk / passenger_sk pointent vers la VERSION du vol et du passager valide
    # le jour de la réservation (valid_from <= booking_date < valid_to), en plus des clés
    # naturelles qui pointent vers l'état courant (dim_flight / dim_passenger).
    con.execute("""
        CREATE OR REPLACE TABLE fact_booking AS
        SELECT
            b.booking_id,
            b.passenger_id,
            ph.passenger_sk,
            b.flight_id,
            fh.flight_sk,
            b.airport_id,
            b.booking_date,
            f.flight_date,
            b.seat_class,
            b.booking_channel,
            b.amount,
            b.currency,
            c.rate_to_eur,
            CAST(round(b.amount * c.rate_to_eur, 2) AS DECIMAL(12, 2)) AS amount_eur,
            date_sub('year', p.birth_date, b.booking_date)              AS passenger_age_at_booking,
            date_diff('day', b.booking_date, f.flight_date)             AS days_before_departure,
            b.insert_timestamp,
            b.update_timestamp
        FROM silver_bookings b
        LEFT JOIN dim_currency c ON c.currency = b.currency
        LEFT JOIN dim_flight f ON f.flight_id = b.flight_id
        LEFT JOIN dim_passenger p ON p.passenger_id = b.passenger_id
        LEFT JOIN dim_flight_history fh
            ON fh.flight_id = b.flight_id
           AND b.booking_date >= fh.valid_from AND b.booking_date < fh.valid_to
        LEFT JOIN dim_passenger_history ph
            ON ph.passenger_id = b.passenger_id
           AND b.booking_date >= ph.valid_from AND b.booking_date < ph.valid_to
    """)

    # --- agg_daily_revenue_airline : chiffre d'affaires par jour (de réservation) et par
    # compagnie. Placée ici car c'est la dernière étape gold de la boucle quotidienne : elle
    # s'appuie sur fact_booking et dim_flight déjà à jour. Timestamps : insert = première
    # réservation contributive vue, update = plus récente -> stables d'une reconstruction à l'autre.
    con.execute("""
        CREATE OR REPLACE TABLE agg_daily_revenue_airline AS
        SELECT
            fb.booking_date,
            f.airline,
            count(*)                             AS nb_bookings,
            sum(fb.amount_eur)                   AS revenue_eur,
            -- Moyenne passée en DECIMAL avant l'arrondi : une division DuckDB renvoie un DOUBLE, et
            -- arrondir directement un DOUBLE tombant pile sur un demi-centime (ex. 376.925) donne
            -- 376.92 ou 376.93 selon la version. En décimal l'arrondi est exact et reproductible.
            CAST(round(CAST(sum(fb.amount_eur) / count(*) AS DECIMAL(18, 6)), 2) AS DECIMAL(12, 2))
                                                 AS avg_amount_eur,
            min(fb.insert_timestamp)             AS insert_timestamp,
            max(fb.update_timestamp)             AS update_timestamp
        FROM fact_booking fb
        JOIN dim_flight f ON f.flight_id = fb.flight_id
        GROUP BY fb.booking_date, f.airline
        ORDER BY fb.booking_date, f.airline
    """)

    # Contrôles de cohérence :
    # - airport_id d'une réservation = aéroport de départ du vol réservé (annoncé dans run_month.py) ;
    # - chaque devise a un taux ;
    # - SCD2 : chaque réservation trouve exactement UNE version de vol et de passager (pas de
    #   trou ni de chevauchement dans les périodes de validité). Un doublon de booking_id
    #   signalerait un chevauchement, un sk NULL un trou.
    bad_airport, no_rate, no_version, n_rows, n_ids = con.execute("""
        SELECT
            count(*) FILTER (WHERE f.origin_airport_id IS DISTINCT FROM fb.airport_id),
            count(*) FILTER (WHERE fb.amount_eur IS NULL),
            count(*) FILTER (WHERE fb.flight_sk IS NULL OR fb.passenger_sk IS NULL),
            count(*),
            count(DISTINCT fb.booking_id)
        FROM fact_booking fb LEFT JOIN dim_flight f ON f.flight_id = fb.flight_id
    """).fetchall()[0]
    if bad_airport or no_rate or no_version or n_rows != n_ids:
        print(f"  [WARN] fact_booking : {bad_airport} airport_id incohérent(s), {no_rate} montant(s) "
              f"sans taux, {no_version} sans version SCD2, {n_rows - n_ids} doublon(s)")

    con.close()


def init():
    ingest_bronze(init=True)
    ingest_silver(init=True)
    ingest_gold()
    print("Réservations (init) ingérés.")


if __name__ == "__main__":
    init()
