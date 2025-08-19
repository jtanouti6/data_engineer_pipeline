#!/bin/bash
# ======================================
# 🧪 quality_monitor.sh - Contrôle Qualité des Données
# ======================================

PIPELINE_ROOT="$(dirname "$0")/.."
STAGING_DIR="$PIPELINE_ROOT/data/staging"
CONFIG_DIR="$PIPELINE_ROOT/config"
QUALITY_LOG="$PIPELINE_ROOT/data/quality/quality_alert.txt"
mkdir -p "$(dirname "$QUALITY_LOG")"

# Chargement des seuils
QUALITY_THRESHOLD=$(yq '.global_threshold' "$CONFIG_DIR/quality_thresholds.yaml")

# Initialisation
> "$QUALITY_LOG"

echo "🔍 Début des contrôles qualité sur $STAGING_DIR"

# Parcours des fichiers en staging
find "$STAGING_DIR" -type f | while read -r file; do
    filename=$(basename "$file")
    echo "➡️  Analyse de $filename"

    # Déduire le type (logs, sessions, users, etc.)
    if [[ "$filename" == api_logs_* ]]; then
        source_type="logs"
    elif [[ "$filename" == sessions_* ]]; then
        source_type="sessions"
    elif [[ "$filename" == users_* ]]; then
        source_type="users"
    elif [[ "$filename" == products_* ]]; then
        source_type="products"
    else
        echo "⚠️  Type inconnu (tag) : $filename" >> "$QUALITY_LOG"
        continue
    fi

    # Appel du validateur Python
    # Appel du script Python de validation (nouvelle version)
    python3 "$PIPELINE_ROOT/processing/data_validator.py" \
        --input "$file" \
        --source "$source_type" \
        --threshold "$QUALITY_THRESHOLD" \
        --check-schema \
        --check-anomalies \
        --check-coherence


    # Récupération du code retour
    if [[ $? -ne 0 ]]; then
        echo "🚫 Fichier rejeté : $filename" >> "$QUALITY_LOG"
    else
        echo "✅ Qualité OK : $filename"
    fi

done

echo "🧪 Contrôle qualité terminé."

exit 0
