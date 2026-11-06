"""
Bonus — orchestration du pipeline avec Dagster.

Chaque fonction ingest_bronze / ingest_silver / ingest_gold des 4 scripts d'ingestion devient un
asset Dagster (une fonction = un asset), partitionné par jour. run_month.py est remplacé par un
backfill lancé depuis l'interface Dagster.

Lancement (depuis ce dossier, pour que dagster.yaml soit pris en compte) :
    pip install -r ../requirements-dagster.txt
    dagster dev
puis http://localhost:3000 > Assets > "Materialize all" > toutes les partitions (backfill).

Choix de conception (détaillés dans DOCUMENTATION.md, section 7) :
- Partitions quotidiennes du 2025-08-31 au 2025-09-30 : la partition 2025-08-31 représente
  l'état initial (dossier init/, jusqu'au 31/08), les 30 suivantes les jours de septembre.
- BackfillPolicy.single_run() : un backfill est exécuté en UN run. Sans elle, Dagster découpe le
  backfill asset par asset (un run par asset et par jour : ~170 runs et 30 min au lieu de 30 s).
  Dans ce run, chaque asset bronze / silver traite la plage de jours reçue dans l'ordre
  chronologique, et chaque gold est reconstruit une seule fois, à la fin.
- Les assets silver dépendent de LEUR PROPRE partition de la veille : le silver est un état
  cumulatif (upsert, désactivations, historique SCD2), le jour J ne peut être chargé qu'après le
  jour J-1. Dagster refuse donc de lancer un jour dont la veille n'est ni chargée ni demandée.
- Les autres dépendances sont celles des données : le silver des réservations contrôle les
  références vers les 3 dimensions du même jour, le gold des passagers lit silver_bookings (date
  de référence de l'âge), le gold des réservations lit dim_flight, dim_passenger et dim_currency.
- Un seul processus à la fois écrit dans warehouse.duckdb (DuckDB n'accepte qu'un écrivain) :
  exécuteur in-process (assets d'un run exécutés l'un après l'autre) + 1 run à la fois
  (dagster.yaml).
"""
import os
import sys
from datetime import date

from dagster import (
    AssetDep,
    AssetExecutionContext,
    AssetKey,
    BackfillPolicy,
    DailyPartitionsDefinition,
    Definitions,
    MaterializeResult,
    TimeWindowPartitionMapping,
    asset,
    in_process_executor,
)

INGESTION_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "ingestion")
sys.path.insert(0, INGESTION_DIR)

import ingest_airports  # noqa: E402
import ingest_bookings  # noqa: E402
import ingest_flights  # noqa: E402
import ingest_passengers  # noqa: E402
from common import get_connection  # noqa: E402
from run_month import init_dim_currency  # noqa: E402

INIT_PARTITION = "2025-08-31"

daily_partitions = DailyPartitionsDefinition(start_date=INIT_PARTITION, end_date="2025-10-01")

ENTITIES = {
    "airports": ingest_airports,
    "flights": ingest_flights,
    "passengers": ingest_passengers,
    "bookings": ingest_bookings,
}

# Tables produites par chaque asset (affichées comme métadonnées dans l'interface).
SILVER_TABLES = {
    "airports": ["silver_airports"],
    "flights": ["silver_flights", "silver_flights_history"],
    "passengers": ["silver_passengers", "silver_passengers_history"],
    "bookings": ["silver_bookings"],
}
GOLD_TABLES = {
    "airports": ["dim_airport"],
    "flights": ["dim_flight", "dim_flight_history"],
    "passengers": ["dim_passenger", "dim_passenger_history"],
    "bookings": ["dim_date", "fact_booking", "agg_daily_revenue_airline"],
}

# Dépendances entre entités (en plus de bronze -> silver -> gold d'une même entité).
SILVER_EXTRA_DEPS = {
    "bookings": [AssetKey(["silver", e]) for e in ("airports", "flights", "passengers")],
}
GOLD_EXTRA_DEPS = {
    "passengers": [AssetKey(["silver", "bookings"])],
    "bookings": [AssetKey(["silver", "flights"]), AssetKey(["gold", "flights"]),
                 AssetKey(["gold", "passengers"]), AssetKey("dim_currency")],
}


def _day_kwargs(partition_key: str) -> dict:
    """Partition Dagster -> arguments des fonctions d'ingestion (init ou jour de septembre)."""
    if partition_key == INIT_PARTITION:
        return {"init": True}
    return {"day": date.fromisoformat(partition_key)}


def _days_in_order(context: AssetExecutionContext) -> list:
    """Partitions demandées dans ce run, triées chronologiquement (format ISO = ordre des dates).

    Avec BackfillPolicy.single_run(), un backfill de N jours est UN run : chaque asset reçoit la
    plage complète et traite les jours lui-même, dans l'ordre. Une matérialisation d'un seul jour
    est simplement une plage de longueur 1.
    """
    return sorted(context.partition_keys)


def _row_counts(tables: list) -> MaterializeResult:
    con = get_connection()
    try:
        counts = {f"lignes {t}": con.execute(f"SELECT count(*) FROM {t}").fetchall()[0][0] for t in tables}
    finally:
        con.close()
    return MaterializeResult(metadata=counts)


@asset(
    key="dim_currency",
    group_name="init",
    description="Table de référence des taux de change (sql/init/dim_currency.sql, fournie).",
)
def dim_currency() -> MaterializeResult:
    init_dim_currency()
    return _row_counts(["dim_currency"])


def _build_entity_assets(name: str, module):
    @asset(
        key=AssetKey(["bronze", name]),
        group_name="bronze",
        partitions_def=daily_partitions,
        backfill_policy=BackfillPolicy.single_run(),
        description=f"{name} : fichier(s) source du jour copiés tels quels dans bronze/.",
    )
    def bronze(context: AssetExecutionContext) -> None:
        for key in _days_in_order(context):
            module.ingest_bronze(**_day_kwargs(key))

    @asset(
        key=AssetKey(["silver", name]),
        group_name="silver",
        partitions_def=daily_partitions,
        backfill_policy=BackfillPolicy.single_run(),
        deps=[
            AssetKey(["bronze", name]),
            # Le silver du jour J n'est chargé qu'après celui de J-1 (état cumulatif).
            AssetDep(
                AssetKey(["silver", name]),
                partition_mapping=TimeWindowPartitionMapping(start_offset=-1, end_offset=-1),
            ),
            *SILVER_EXTRA_DEPS.get(name, []),
        ],
        description=f"{name} : chargement silver du jour (relit bronze/).",
    )
    def silver(context: AssetExecutionContext) -> MaterializeResult:
        days = _days_in_order(context)
        for key in days:  # ordre chronologique obligatoire : le silver est un état cumulatif
            module.ingest_silver(**_day_kwargs(key))
        context.log.info(f"silver/{name} : {len(days)} jour(s) chargé(s), de {days[0]} à {days[-1]}")
        return _row_counts(SILVER_TABLES[name])

    @asset(
        key=AssetKey(["gold", name]),
        group_name="gold",
        partitions_def=daily_partitions,
        backfill_policy=BackfillPolicy.single_run(),
        deps=[AssetKey(["silver", name]), *GOLD_EXTRA_DEPS.get(name, [])],
        description=f"{name} : reconstruction gold depuis l'état courant du silver.",
    )
    def gold() -> MaterializeResult:
        # Le gold ne dépend que de l'état courant du silver, pas d'une date : une seule
        # reconstruction suffit, quel que soit le nombre de jours traités dans le run.
        module.ingest_gold()
        return _row_counts(GOLD_TABLES[name])

    return [bronze, silver, gold]


all_assets = [dim_currency]
for _name, _module in ENTITIES.items():
    all_assets += _build_entity_assets(_name, _module)

defs = Definitions(assets=all_assets, executor=in_process_executor)
