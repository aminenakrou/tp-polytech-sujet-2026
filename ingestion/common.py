"""
Utilitaires partagés par les scripts d'ingestion. `get_connection` est fourni tel quel ;
`fetch_csv` est à vous d'implémenter (cf. TODO)
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
