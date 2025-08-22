#!/usr/bin/env python3
# Traitement des logs API - version modulaire avec chunks

import os
import sys
import argparse
import pandas as pd
from datetime import datetime

# Ajout du chemin racine pour import des modules de transformations
pipeline_root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, pipeline_root)

from transformations.data_cleaner import clean_api_logs
from transformations.data_enricher import enrich_api_logs
from transformations.data_aggregator import aggregate_api_logs
from transformations.data_formatter import export_api_logs_partitioned

# 🎯 Arguments CLI
parser = argparse.ArgumentParser(description="Traitement des logs API")
parser.add_argument('--input', required=True, help="Fichier JSONL (logs API ligne par ligne)")
parser.add_argument('--chunksize', type=int, default=100_000, help="Taille des chunks (lignes)")
args = parser.parse_args()

input_path = args.input
chunksize = args.chunksize
print(f"🐛 chunksize reçu via argparse : {chunksize}")
if not os.path.exists(input_path):
    print(f"❌ Fichier introuvable : {input_path}")
    sys.exit(1)

# 📥 Lecture du JSON ligne par ligne en chunks
try:
    chunks = pd.read_json(input_path, lines=True, chunksize=chunksize)
except Exception as e:
    print(f"❌ Erreur de lecture JSONL en chunks : {e}")
    sys.exit(1)

# 💾 Accumulation des morceaux nettoyés et enrichis
processed_chunks = []

for i, chunk in enumerate(chunks):
    print(f"🔢 Traitement du chunk {i + 1}...")
    chunk_cleaned = clean_api_logs(chunk)
    chunk_enriched = enrich_api_logs(chunk_cleaned, input_path, append=True)
    processed_chunks.append(chunk_enriched)

# 🧱 Concaténation + agrégation
df_full = pd.concat(processed_chunks, ignore_index=True)
df_agg = aggregate_api_logs(df_full)
export_api_logs_partitioned(df_agg, input_path)
