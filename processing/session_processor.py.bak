#!/usr/bin/env python3
# Analyse des sessions utilisateur (lecture, nettoyage, enrichissement, agrégation, export)

import sys
import argparse
import pandas as pd
import os
from datetime import datetime

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
chunksize = args.chunksize  # Par défaut None si non fourni

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

# Liste qui va contenir les DataFrames traités si on utilise les chunks
processed_chunks = []

# ==============================
# 📚 Lecture du CSV par chunks (si demandé)
# ==============================

try:
    if chunksize:
        # Lecture et traitement par morceaux (pour les gros fichiers)
        for i, chunk in enumerate(pd.read_csv(input_path, chunksize=chunksize)):
            print(f"🔹 Chunk {i+1} lu ({len(chunk)} lignes)")

            # Nettoyage du chunk
            chunk = clean_session_data(chunk)

            # Enrichissement (ajout d'infos dérivées ou calculées)
            chunk = enrich_session_data(chunk, input_path)

            # On stocke le résultat pour concaténation ultérieure
            processed_chunks.append(chunk)
    else:
        # Lecture directe en mémoire si pas de chunksize spécifié
        df = pd.read_csv(input_path)
        df = clean_session_data(df)
        df = enrich_session_data(df, input_path)
        processed_chunks.append(df)

except Exception as e:
    print(f"❌ Erreur de lecture ou traitement : {e}")
    sys.exit(1)

# ==============================
# 🧩 Fusion des morceaux
# ==============================

df_all = pd.concat(processed_chunks, ignore_index=True)

# ==============================
# 📊 Agrégation multi-dimensionnelle
# ==============================

# Dimensions possibles sur lesquelles on veut des indicateurs agrégés
dimensions = ["device_type", "browser", "referrer", "country", "city", "conversion"]
df_agg = aggregate_session_data(df_all, dimensions)

# ==============================
# 💾 Export final partitionné par date
# ==============================

export_session_data_partitioned(df_agg, input_path)

# ==============================
# ✅ Fin du traitement
# ==============================

print("✅ Traitement des sessions terminé.")
sys.exit(0)
