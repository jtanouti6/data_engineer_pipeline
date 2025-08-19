#!/usr/bin/env python3
# ✅ Validation avancée avec règles de qualité dynamiques

import sys
import argparse
import pandas as pd
import os
import json
import yaml
from datetime import datetime

# ===============================
# 🌟 CLI Arguments
# ===============================

parser = argparse.ArgumentParser(description="Validation de qualité des fichiers de données")
parser.add_argument('--input', required=True, help="Fichier CSV à valider")
parser.add_argument('--source', required=True, help="Type de données : logs, sessions, products, users")
parser.add_argument('--threshold', type=int, help="Seuil de complétude minimum (%)")
parser.add_argument('--check-schema', action='store_true', help="Valider le schéma")
parser.add_argument('--check-anomalies', action='store_true', help="Détecter les anomalies statistiques")
parser.add_argument('--check-coherence', action='store_true', help="Contrôles inter-fichiers")
args = parser.parse_args()

# ===============================
# 💾 Chargement des fichiers
# ===============================

if not os.path.exists(args.input):
    print(f"❌ Fichier introuvable : {args.input}")
    sys.exit(1)

ext = os.path.splitext(args.input)[1].lower()

try:
    if ext == ".csv":
        df = pd.read_csv(args.input)
    elif ext == ".json":
            try:
                df = pd.read_json(args.input, lines=True)
            except ValueError:
                df = pd.read_json(args.input, lines=False)
    elif ext == ".xlsx":
        df = pd.read_excel(args.input, engine="openpyxl")
    else:
        raise ValueError(f"Format non supporté : {ext}")
except Exception as e:
    print(f"❌ Erreur de lecture : {e}")
    sys.exit(1)

filename = os.path.basename(args.input)
validation_passed = True
errors = []

# ===============================
# 📚 Chargement des configurations
# ===============================
PIPELINE_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
CONFIG_DIR = os.path.join(PIPELINE_ROOT, "config")

try:
    with open(os.path.join(CONFIG_DIR, "data_schemas.json")) as f:
        schema_config = json.load(f)
    with open(os.path.join(CONFIG_DIR, "business_rules.yaml")) as f:
        business_rules = yaml.safe_load(f)
    with open(os.path.join(CONFIG_DIR, "quality_thresholds.yaml")) as f:
        thresholds = yaml.safe_load(f)
        global_threshold = thresholds.get("global_threshold", 95)
except Exception as e:
    print(f"❌ Erreur chargement des fichiers de configuration : {e}")
    sys.exit(1)

# ===============================
# 🔢 Validation du schéma
# ===============================

source_schema = schema_config.get(args.source)

if source_schema is None or "required_columns" not in source_schema:
    errors.append(f"⚠️ Aucun schéma défini pour la source : {args.source}")
    validation_passed = False
else:
    expected_columns = list(source_schema["required_columns"].keys())
    missing_columns = [col for col in expected_columns if col not in df.columns]
    if missing_columns:
        for col in missing_columns:
            errors.append(f"Colonne manquante (schema) : {col}")
        validation_passed = False


# ===============================
# 🔍 Règles métier (business rules)
# ===============================

if args.source in business_rules:
    rules = business_rules[args.source]
    for col, constraints in rules.items():
        if col not in df.columns:
            continue
        for rule, value in constraints.items():
            if rule == "allowed_range":
                min_v, max_v = value
                violations = df[~df[col].between(min_v, max_v)]
                if not violations.empty:
                    errors.append(f"{len(violations)} valeurs hors intervalle {value} pour '{col}'")
                    validation_passed = False

            elif rule == "min_value":
                violations = df[df[col] < value]
                if not violations.empty:
                    errors.append(f"{len(violations)} valeurs < {value} pour '{col}'")
                    validation_passed = False

            elif rule == "max_value":
                violations = df[df[col] > value]
                if not violations.empty:
                    errors.append(f"{len(violations)} valeurs > {value} pour '{col}'")
                    validation_passed = False

            elif rule == "allowed_values":
                violations = df[~df[col].isin(value)]
                if not violations.empty:
                    errors.append(f"{len(violations)} valeurs non autorisées pour '{col}'")
                    validation_passed = False

            elif rule == "not_allowed_values":
                violations = df[df[col].isin(value)]
                if not violations.empty:
                    errors.append(f"{len(violations)} valeurs interdites pour '{col}'")
                    validation_passed = False

# ===============================
# 🔢 Anomalies simples
# ===============================

if args.check_anomalies:
    if 'duration_min' in df.columns:
        anomalies = df[df['duration_min'] > 180]
        if not anomalies.empty:
            errors.append(f"{len(anomalies)} sessions > 3h détectées")
            validation_passed = False

    if 'total_spent' in df.columns:
        max_total = df['total_spent'].max()
        if max_total > 10000:
            errors.append(f"Montant très élevé : {max_total}")
            validation_passed = False

# ===============================
# 🔄 Cohérence inter-fichiers
# ===============================

# if args.check_coherence and 'user_id' in df.columns:
#     duplicated_users = df[df['user_id'].duplicated(keep=False)]
#     if not duplicated_users.empty:
#         sample_duplicates = duplicated_users['user_id'].unique()[:10].tolist()
#         errors.append(f"Duplications de user_id détectées : exemples {sample_duplicates}")
#         validation_passed = False


# ===============================
# 📊 Complétude
# ===============================

total_cells = df.shape[0] * df.shape[1]
missing_cells = df.isnull().sum().sum()
completeness = 100 * (1 - (missing_cells / total_cells))
threshold = args.threshold if args.threshold else global_threshold

if completeness < threshold:
    errors.append(f"Complétude insuffisante ({completeness:.2f}%) < seuil {threshold}%")
    validation_passed = False
else:
    print(f"✅ Complétude : {completeness:.2f}%")

# ===============================
# 📃 Rapport JSON
# ===============================

report = {
    "filename": filename,
    "source": args.source,
    "rows": int(df.shape[0]),
    "columns": int(df.shape[1]),
    "missing_values": int(missing_cells),
    "completeness": round(completeness, 2),
    "threshold": threshold,
    "status": "passed" if validation_passed else "failed",
    "validated_at": datetime.utcnow().isoformat() + "Z",
    "errors": errors if errors else None
}

# 🔧 Déduire le chemin racine du projet (PIPELINE_ROOT)
script_dir = os.path.abspath(os.path.dirname(__file__))
pipeline_root = os.path.abspath(os.path.join(script_dir, ".."))

# 📁 Chemin vers le dossier quality
quality_dir = os.path.join(pipeline_root, "data", "quality")
os.makedirs(quality_dir, exist_ok=True)

# 📄 Nom du rapport JSON
report_path = os.path.join(quality_dir, f"validation_report_{filename}.json")

# 💾 Écriture
with open(report_path, "w", encoding="utf-8") as f:
    json.dump(report, f, indent=4, ensure_ascii=False)

print(f"📝 Rapport sauvegardé : {report_path}")

sys.exit(0 if validation_passed else 1)
