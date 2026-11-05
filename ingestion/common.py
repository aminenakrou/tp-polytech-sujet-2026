"""
Utilitaires partagés par les scripts d'ingestion :
- `fetch_csv` : téléchargement d'un fichier source vers bronze/ (avec cache) puis relecture disque ;
- `get_connection` : connexion à l'entrepôt DuckDB local (fourni) ;
- `upsert_snapshot` : chargement silver commun aux dimensions livrées en extrait complet
  (upsert sur vrai changement + désactivation des lignes disparues).
"""
import os
import urllib.request

import duckdb
import pandas as pd

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
BRONZE_DIR = os.path.join(BASE_DIR, "bronze")
DB_PATH = os.path.join(BASE_DIR, "warehouse.duckdb")
RAW_BASE = "https://raw.githubusercontent.com/kevinl75/tp-polytech-dataset/main"

DOWNLOAD_TIMEOUT_S = 30


def bronze_path(subdir: str, filename: str) -> str:
    """Chemin local du fichier brut : bronze/<subdir>/<filename> (même arborescence que la source)."""
    return os.path.join(BRONZE_DIR, subdir, filename)


def fetch_csv(subdir: str, filename: str) -> pd.DataFrame:
    """Renvoie le contenu de <subdir>/<filename> sous forme de DataFrame pandas.

    - Si le fichier n'est pas encore en bronze, il est téléchargé et écrit TEL QUEL sur disque
      (octets bruts, sans passer par pandas : aucune transformation, pas même d'encodage).
    - S'il est déjà présent, on ne le retélécharge pas : le bronze sert de cache et permet de
      tout rejouer hors-ligne.
    - Dans tous les cas, le DataFrame renvoyé est relu depuis le disque, jamais depuis le réseau :
      ce qui est lu est exactement ce qui est stocké.
    """
    local_path = bronze_path(subdir, filename)

    if not os.path.exists(local_path):
        os.makedirs(os.path.dirname(local_path), exist_ok=True)
        url = f"{RAW_BASE}/{subdir}/{filename}"
        with urllib.request.urlopen(url, timeout=DOWNLOAD_TIMEOUT_S) as response:
            content = response.read()
        # Écriture dans un fichier temporaire puis renommage : si le téléchargement plante en
        # cours de route, on ne laisse jamais un fichier tronqué en bronze (qui serait ensuite
        # considéré comme "déjà téléchargé" au prochain lancement).
        tmp_path = local_path + ".tmp"
        with open(tmp_path, "wb") as f:
            f.write(content)
        os.replace(tmp_path, local_path)

    # dtype=str : en bronze on ne fait aucune interprétation des types (dates, montants...),
    # tout reste du texte. Le typage est la responsabilité de la couche silver.
    return pd.read_csv(local_path, dtype=str)


def get_connection() -> duckdb.DuckDBPyConnection:
    return duckdb.connect(DB_PATH)


def upsert_snapshot(con, table: str, key: str, cols: list, snapshot_date):
    """Applique un snapshot COMPLET (table temporaire `snapshot`) sur une dimension silver.

    Utilisé par les dimensions livrées en extrait complet quotidien (vols, passagers) : la source
    ne dit pas ce qui a changé, c'est la comparaison snapshot du jour <-> état silver qui le révèle.

    1. Upsert : clé inconnue -> insertion ; clé connue -> mise à jour, mais UNIQUEMENT si au moins
       une valeur a réellement changé (ou si la ligne était désactivée et réapparaît). Une ligne
       identique à la veille n'est pas retouchée : son update_timestamp garde donc un vrai sens
       ("dernière modification constatée") et rejouer le même jour ne modifie rien (idempotence).
       insert_timestamp n'est jamais dans le SET : il reste figé à la première apparition.
    2. Désactivation : les clés actives absentes du snapshot ont disparu de la source. On ne les
       supprime jamais physiquement (des réservations les référencent) : is_active passe à false
       et deleted_date est figée au jour où la disparition est constatée.
    """
    all_cols = [key] + cols
    col_list = ", ".join(all_cols)
    set_clause = ", ".join(f"{c} = excluded.{c}" for c in cols)
    # IS DISTINCT FROM (et pas <>) : compare correctement les NULL (NULL <> 'x' vaut NULL, pas true).
    has_changed = " OR ".join(f"{table}.{c} IS DISTINCT FROM excluded.{c}" for c in cols)

    con.begin()  # les deux étapes forment un tout : soit les deux passent, soit aucune
    con.execute(f"""
        INSERT INTO {table} ({col_list}, is_active, deleted_date, insert_timestamp, update_timestamp)
        SELECT {col_list}, true, NULL, now(), now()
        FROM snapshot
        ON CONFLICT ({key}) DO UPDATE SET
            {set_clause}, is_active = true, deleted_date = NULL, update_timestamp = now()
        WHERE {table}.is_active = false OR {has_changed}
    """)
    con.execute(
        f"""
        UPDATE {table} SET is_active = false, deleted_date = ?, update_timestamp = now()
        WHERE is_active = true AND {key} NOT IN (SELECT {key} FROM snapshot)
        """,
        [snapshot_date],
    )
    con.commit()
