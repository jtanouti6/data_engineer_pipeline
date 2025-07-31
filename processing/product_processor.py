#!/usr/bin/env python3
# Traitement des données produits

import argparse
import os
import sys
import pandas as pd

# 📁 Ajout du chemin racine pour import relatif
pipeline_root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, pipeline_root)

# === CLI ===
parser = argparse.ArgumentParser(description="Traitement des données produits")
parser.add_argument('--input', required=True, help="Fichier CSV ou Excel contenant les données produits")
parser.add_argument('--chunksize', type=int, default=None, help="Taille de chunk (en lignes) pour les gros fichiers CSV")
args = parser.parse_args()
input_path = args.input
chunksize = args.chunksize

# === Lecture avec chunks ===
if not os.path.exists(input_path):
    print(f"❌ Fichier introuvable : {input_path}")
    sys.exit(1)

try:
    if input_path.endswith(".csv"):
        if chunksize:
            df_chunks = []
            for i, chunk in enumerate(pd.read_csv(input_path, chunksize=chunksize)):
                print(f"🔹 Chunk {i+1} ({len(chunk)} lignes)")

                from transformations.data_cleaner import clean_product_data
                from transformations.data_enricher import enrich_product_data

                chunk = clean_product_data(chunk)
                chunk = enrich_product_data(chunk, input_path)
                df_chunks.append(chunk)

            df = pd.concat(df_chunks, ignore_index=True)
        else:
            df = pd.read_csv(input_path)
            from transformations.data_cleaner import clean_product_data
            from transformations.data_enricher import enrich_product_data
            df = clean_product_data(df)
            df = enrich_product_data(df, input_path)

    elif input_path.endswith(".xlsx"):
        df = pd.read_excel(input_path)
        from transformations.data_cleaner import clean_product_data
        from transformations.data_enricher import enrich_product_data
        df = clean_product_data(df)
        df = enrich_product_data(df, input_path)

    else:
        raise ValueError("Format de fichier non supporté (CSV ou XLSX attendu)")

except Exception as e:
    print(f"❌ Erreur de lecture ou traitement : {e}")
    sys.exit(1)

print("✅ Lecture, nettoyage et enrichissement terminés.")

# === Agrégation
from transformations.data_aggregator import aggregate_product_data
df_agg = aggregate_product_data(df)

# === Export
from transformations.data_formatter import export_product_data_partitioned
export_product_data_partitioned(df_agg, input_path)

print("✅ Traitement des produits terminé.")
sys.exit(0)
