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


SCD2_OPEN_END = "9999-12-31"   # valid_to d'une version encore en cours
SCD2_INIT_START = "1900-01-01"  # valid_from des versions issues de init/ (historique antérieur inconnu)


def apply_scd2(con, history_table: str, sk: str, key: str, cols: list, snapshot_date, init: bool):
    """Historise (SCD type 2) le snapshot complet du jour (table temporaire `snapshot`).

    Chaque version d'une ligne est conservée avec sa période de validité [valid_from, valid_to[ :
    borne de début incluse, borne de fin EXCLUE, valid_to = 9999-12-31 pour la version en cours.
    Une ligne du snapshot du jour D décrit l'état de la source le jour D : une version créée à
    partir du snapshot D est donc valide à partir de D (les réservations du jour D référencent
    justement les vols / passagers tels qu'ils sont dans les fichiers du jour D).

    1. Fermeture : la version en cours d'une clé est close (valid_to = D, is_current = false)
       si ses valeurs diffèrent du snapshot (modification) ou si la clé a disparu (suppression,
       la version est close sans successeur).
    2. Ouverture : chaque clé du snapshot sans version en cours (nouvelle clé, clé modifiée qui
       vient d'être close, clé qui réapparaît) reçoit une nouvelle version [D, 9999-12-31[.
       Les versions issues de init/ commencent en 1900-01-01 : on ne connaît pas l'historique
       antérieur, l'état initial est réputé valide depuis toujours.

    Clé de substitution (surrogate key) `<clé>@<valid_from>` : déterministe (identique à chaque
    reconstruction) et unique, puisqu'une clé a au plus une version par jour.

    Garde-fou d'idempotence : contrairement à l'upsert SCD1, rejouer un jour ANCIEN casserait
    l'historique (on fermerait des versions dans le passé). On mémorise donc le dernier snapshot
    historisé (table silver_scd2_watermark) et on ignore tout snapshot daté de ce jour ou avant.
    Rejouer le mois complet (run_month.py) est ainsi sans effet sur l'historique.
    """
    con.execute("""
        CREATE TABLE IF NOT EXISTS silver_scd2_watermark (
            table_name VARCHAR PRIMARY KEY,
            last_snapshot_date DATE,
            insert_timestamp TIMESTAMP,
            update_timestamp TIMESTAMP
        )
    """)
    last = con.execute(
        "SELECT last_snapshot_date FROM silver_scd2_watermark WHERE table_name = ?", [history_table]
    ).fetchall()
    if last and snapshot_date <= last[0][0]:
        return  # déjà historisé : rien à faire

    valid_from = SCD2_INIT_START if init else snapshot_date.isoformat()
    col_list = ", ".join(cols)
    has_changed = " OR ".join(f"h.{c} IS DISTINCT FROM s.{c}" for c in cols)

    con.begin()
    con.execute(
        f"""
        UPDATE {history_table} AS h
        SET valid_to = CAST(? AS DATE), is_current = false, update_timestamp = now()
        WHERE h.is_current AND (
            NOT EXISTS (SELECT 1 FROM snapshot s WHERE s.{key} = h.{key})
            OR EXISTS (SELECT 1 FROM snapshot s WHERE s.{key} = h.{key} AND ({has_changed}))
        )
        """,
        [valid_from],
    )
    con.execute(
        f"""
        INSERT INTO {history_table} ({sk}, {key}, {col_list}, valid_from, valid_to, is_current,
                                     insert_timestamp, update_timestamp)
        SELECT s.{key} || '@' || ?, s.{key}, {", ".join("s." + c for c in cols)},
               CAST(? AS DATE), DATE '{SCD2_OPEN_END}', true, now(), now()
        FROM snapshot s
        WHERE NOT EXISTS (SELECT 1 FROM {history_table} h WHERE h.{key} = s.{key} AND h.is_current)
        """,
        [valid_from, valid_from],
    )
    con.execute(
        """
        INSERT INTO silver_scd2_watermark VALUES (?, ?, now(), now())
        ON CONFLICT (table_name) DO UPDATE SET
            last_snapshot_date = excluded.last_snapshot_date, update_timestamp = now()
        """,
        [history_table, snapshot_date],
    )
    con.commit()
