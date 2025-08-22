#!/usr/bin/env python3
# Analyse des sessions utilisateur (lecture, nettoyage, enrichissement, agrégation, export)

import sys
import argparse
import pandas as pd
import os

# 📁 Ajout du chemin racine du projet pour permettre l'import de modules dans /transformations
pipeline_root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, pipeline_root)

# ==============================
# 🎯 Lecture des arguments CLI
# ==============================
parser = argparse.ArgumentParser(description="Analyse des sessions web")
parser.add_argument('--input', required=True, help="Fichier CSV des sessions utilisateur")
parser.add_argument('--chunksize', type=int, default=None, help="Taille des chunks à lire (nombre de lignes)")
args = parser.parse_args()
input_path = args.input
chunksize = args.chunksize  # None par défaut

# ==============================
# 📥 Vérification du fichier d'entrée
# ==============================
if not os.path.exists(input_path):
    print(f"❌ Fichier introuvable : {input_path}")
    sys.exit(1)

# ==============================
# 📦 Imports des fonctions métiers
# ==============================
from transformations.data_cleaner import clean_session_data
from transformations.data_enricher import enrich_session_data
from transformations.data_aggregator import aggregate_session_data
from transformations.data_formatter import export_session_data_partitioned

processed_chunks = []

# ==============================
# 📚 Lecture du CSV (chunks ou full)
# ==============================
try:
    if chunksize:
        for i, chunk in enumerate(pd.read_csv(input_path, chunksize=chunksize)):
            print(f"🔹 Chunk {i+1} lu ({len(chunk)} lignes)")
            chunk = clean_session_data(chunk)
            # ⚠️ enrich_session_data ne doit plus écrire sur disque
            chunk = enrich_session_data(chunk)
            processed_chunks.append(chunk)
    else:
        df = pd.read_csv(input_path)
        df = clean_session_data(df)
        df = enrich_session_data(df)  # ⚠️ sans export ici
        processed_chunks.append(df)
except Exception as e:
    print(f"❌ Erreur de lecture ou traitement : {e}")
    sys.exit(1)

# ==============================
# 🧩 Fusion des morceaux
# ==============================
df_all = pd.concat(processed_chunks, ignore_index=True)

# ==============================
# 💾 Export enrichi (append unique ici)
# ==============================
enriched_dir = os.path.join(pipeline_root, "data", "processed", "enriched")
os.makedirs(enriched_dir, exist_ok=True)
enriched_path = os.path.join(enriched_dir, "sessions_enriched.csv")

# Évite d’écraser : append + header si nouveau fichier
write_header = not os.path.exists(enriched_path)
df_all.to_csv(enriched_path, index=False, mode="a", header=write_header)
print(f"💾 Données de session enrichies exportées (append) : {enriched_path}")

# (Optionnel anti-duplicates si tu relances souvent la pipeline)
# if "session_id" in df_all.columns:
#     df_all = df_all.drop_duplicates(subset=["session_id"])

# ==============================
# 📊 Agrégation multi-dimensionnelle
# ==============================
dimensions = ["device_type", "browser", "referrer", "country", "city", "conversion"]
df_agg = aggregate_session_data(df_all, dimensions)

# ==============================
# 💾 Export agrégé partitionné par date
# ==============================
export_session_data_partitioned(df_agg, input_path)

print("✅ Traitement des sessions terminé.")
sys.exit(0)
