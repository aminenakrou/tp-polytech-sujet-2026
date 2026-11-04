"""
À compléter : ingestion de l'entité `bookings`, découpée en 3 fonctions bronze/silver/gold
(même structure que `ingest_airports.py`, à utiliser comme modèle).
"""
from datetime import date

from common import fetch_csv


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
    # TODO : créer silver_bookings (colonnes du CSV source + insert_timestamp/update_timestamp, cf. ingest_airports.py).
    raise NotImplementedError


def ingest_silver(day: date = None, init: bool = False):
    # TODO : relire le fichier du jour depuis bronze/ et charger les lignes dans silver_bookings.
    raise NotImplementedError


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
