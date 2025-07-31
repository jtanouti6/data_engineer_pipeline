#!/usr/bin/env python3
# Traitement des ventes utilisateur

import sys
import argparse
import pandas as pd
import os
from datetime import datetime

# 📁 Ajout du chemin racine pour import relatif
pipeline_root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, pipeline_root)

# ==============================
# 🎯 Argument en ligne de commande
# ==============================

parser = argparse.ArgumentParser(description="Analyse des ventes web")
parser.add_argument('--input', required=True, help="Fichier CSV des ventes utilisateur")
parser.add_argument('--chunksize', type=int, default=None, help="Taille de chunk pour traitement par morceaux")
args = parser.parse_args()
input_path = args.input
chunksize = args.chunksize

# ==============================
# 📥 Lecture du fichier (par chunk ou complet)
# ==============================

if not os.path.exists(input_path):
    print(f"❌ Fichier introuvable : {input_path}")
    sys.exit(1)

from transformations.data_cleaner import clean_user_data
from transformations.data_enricher import enrich_user_data

try:
    if chunksize:
        chunk_iter = pd.read_csv(input_path, chunksize=chunksize)
        df_list = []

        for i, chunk in enumerate(chunk_iter):
            print(f"🔹 Chunk {i+1} en traitement ({len(chunk)} lignes)")
            chunk = clean_user_data(chunk)
            chunk = enrich_user_data(chunk, input_path)
            df_list.append(chunk)

        df = pd.concat(df_list, ignore_index=True)
    else:
        df = pd.read_csv(input_path)
        df = clean_user_data(df)
        df = enrich_user_data(df, input_path)

    print("🧹 Nettoyage + ✨ Enrichissement OK")

except Exception as e:
    print(f"❌ Erreur de lecture ou de traitement : {e}")
    sys.exit(1)

# ============================
# 📊 Agrégation
# ============================
from transformations.data_aggregator import aggregate_user_data
df_agg = aggregate_user_data(df)
print("📊 Agrégation OK")

# ============================
# 💾 Export partitionné
# ============================
from transformations.data_formatter import export_user_data_partitioned
export_user_data_partitioned(df_agg, input_path)
print("💾 Export OK")

print("✅ Traitement des ventes terminé.")
sys.exit(0)
