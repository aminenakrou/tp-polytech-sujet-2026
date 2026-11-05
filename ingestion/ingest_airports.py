"""
Ingestion de l'entité `airports` (la dimension la plus stable du jeu de données : extrait complet
quotidien, 1 ajout et 1 correction sur le mois).
- bronze : snapshot du jour copié tel quel dans bronze/ ;
- silver : upsert + désactivation des aéroports disparus (common.upsert_snapshot) ;
- gold   : dim_airport, copie de l'état courant du silver.

Découpé en 3 fonctions correspondant aux 3 couches du médaillon — un peu comme 3 tâches
distinctes qu'exécuterait un pipeline orchestré (cf. bonus Dagster) : chaque fonction relit ce
que la précédente a persisté (fichier bronze, puis table silver), elle ne reçoit jamais rien
directement d'un objet Python de l'étape d'avant.
"""
from datetime import date

from common import fetch_csv, get_connection, upsert_snapshot

AIRPORT_COLS = ["iata_code", "airport_name", "city", "country"]


def _snapshot_file(day: date = None, init: bool = False):
    if init:
        return "init", "airports.csv"
    assert day is not None
    return "2025-09", f"airports_{day.isoformat()}.csv"


def ingest_bronze(day: date = None, init: bool = False):
    """Rapatrie le snapshot complet des aéroports du jour (ou de init/) dans bronze/."""
    subdir, filename = _snapshot_file(day, init)
    fetch_csv(subdir, filename)


def create_silver_table(con):
    # insert_timestamp / update_timestamp : colonnes techniques à reproduire sur TOUTES vos
    # tables silver (et, comme on le voit dans ingest_gold ci-dessous, elles suivent
    # automatiquement en gold puisqu'on y fait un simple `SELECT *` depuis le silver).
    # insert_timestamp = quand la ligne a été vue pour la première fois, ne change plus jamais.
    # update_timestamp = à chaque fois que la ligne est retouchée (upsert ou désactivation).
    # deleted_date = NULL tant que la ligne est active, puis figée à la date de désactivation.
    con.execute("""
        CREATE TABLE IF NOT EXISTS silver_airports (
            airport_id VARCHAR PRIMARY KEY,
            iata_code VARCHAR,
            airport_name VARCHAR,
            city VARCHAR,
            country VARCHAR,
            is_active BOOLEAN,
            deleted_date DATE,
            insert_timestamp TIMESTAMP,
            update_timestamp TIMESTAMP
        )
    """)


def ingest_silver(day: date = None, init: bool = False):
    """Relit le snapshot depuis bronze/ et l'upsert dans silver_airports."""
    subdir, filename = _snapshot_file(day, init)
    df = fetch_csv(subdir, filename)  # déjà en cache local : pas de nouveau téléchargement
    snapshot_date = date(2025, 8, 31) if init else day

    con = get_connection()
    create_silver_table(con)
    con.register("snapshot_raw", df)
    con.execute(f"""
        CREATE OR REPLACE TEMP TABLE snapshot AS
        SELECT airport_id, {", ".join(AIRPORT_COLS)} FROM snapshot_raw
    """)

    # Modification par rapport au squelette fourni : on utilise le même upsert que les vols et
    # les passagers (common.upsert_snapshot). Différence de comportement : le squelette
    # rafraîchissait update_timestamp à CHAQUE passage, même sans changement de valeur ; ici il
    # n'est mis à jour que si une valeur a réellement changé (ou sur désactivation /
    # réactivation). update_timestamp signifie donc "dernière modification constatée" de façon
    # homogène sur les 3 dimensions, et rejouer un jour déjà traité ne modifie rien.
    # Principes conservés : insert_timestamp jamais écrasé, réactivation propre d'une ligne qui
    # réapparaît (deleted_date remis à NULL), désactivation sans suppression physique avec
    # deleted_date figée au jour où la disparition est constatée.
    upsert_snapshot(con, "silver_airports", "airport_id", AIRPORT_COLS, snapshot_date)

    con.unregister("snapshot_raw")
    con.close()


def ingest_gold():
    """Reconstruit dim_airport à partir de l'état courant de silver_airports."""
    con = get_connection()
    # SELECT * : insert_timestamp/update_timestamp suivent automatiquement en gold, sans rien
    # ajouter de spécifique ici.
    con.execute("CREATE OR REPLACE TABLE dim_airport AS SELECT * FROM silver_airports")
    con.close()


def init():
    ingest_bronze(init=True)
    ingest_silver(init=True)
    ingest_gold()
    print("Aéroports (init) ingérés.")


if __name__ == "__main__":
    init()
