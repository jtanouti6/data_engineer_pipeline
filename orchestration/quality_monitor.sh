#!/bin/bash
# ======================================
# 🧪 quality_monitor.sh - Contrôle Qualité des Données
# ======================================

# 📍 Chemins
PIPELINE_ROOT="$(dirname "$0")/.."
STAGING_DIR="$PIPELINE_ROOT/data/staging"
CONFIG_DIR="$PIPELINE_ROOT/config"
QUALITY_DIR="$PIPELINE_ROOT/data/quality"
QUALITY_LOG="$QUALITY_DIR/quality_alert.txt"

mkdir -p "$QUALITY_DIR"
> "$QUALITY_LOG"

# ✅ Paramètres : seuil + workers
QUALITY_THRESHOLD="$1"
QUALITY_WORKERS="$2"

# Fallbacks
[ -z "$QUALITY_THRESHOLD" ] && QUALITY_THRESHOLD=90
[ -z "$QUALITY_WORKERS" ] && QUALITY_WORKERS=4

echo "📊 Contrôle Qualité : Seuil=$QUALITY_THRESHOLD% | Workers=$QUALITY_WORKERS"
echo "🔍 Dossier à analyser : $STAGING_DIR"

# Fonction de validation pour un fichier
validate_file() {
    file="$1"
    filename=$(basename "$file")

    # Déduire le type de source
    if [[ "$filename" == api_logs_* ]]; then
        source_type="logs"
    elif [[ "$filename" == sessions_* ]]; then
        source_type="sessions"
    elif [[ "$filename" == users_* ]]; then
        source_type="users"
    elif [[ "$filename" == products_* ]]; then
        source_type="products"
    else
        echo "⚠️  Type inconnu : $filename" >> "$QUALITY_LOG"
        return
    fi

    # Appel du validateur Python
    python3 "$PIPELINE_ROOT/processing/data_validator.py" \
        --input "$file" \
        --source "$source_type" \
        --threshold "$QUALITY_THRESHOLD" \
        --check-schema \
        --check-anomalies \
        --check-coherence

    if [[ $? -ne 0 ]]; then
        echo "🚫 Fichier rejeté : $filename" >> "$QUALITY_LOG"
    else
        echo "✅ Qualité OK : $filename"
    fi
}

export -f validate_file
export PIPELINE_ROOT CONFIG_DIR QUALITY_LOG QUALITY_THRESHOLD

# Lancement en parallèle avec xargs
find "$STAGING_DIR" -type f | xargs -P "$QUALITY_WORKERS" -I{} bash -c 'validate_file "$@"' _ {}

echo "✅ Contrôle qualité terminé."
exit 0
