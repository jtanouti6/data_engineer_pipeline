#!/bin/bash
# Script de gestion des workers pour lancer les traitements Python

NB_WORKERS="$1"
chunk_size="$2"
[ -z "$NB_WORKERS" ] && NB_WORKERS=1
[ -z "$chunk_size" ] && chunk_size=100000

PIPELINE_ROOT="$(dirname "$0")/.."
STAGING_DIR="$PIPELINE_ROOT/data/staging"
LOG_FILE="$PIPELINE_ROOT/logs/pipeline.log"

echo "⚙️ Lancement des workers ($NB_WORKERS)..." | tee -a "$LOG_FILE"
echo "ℹ️ Chunk size global : $chunk_size" | tee -a "$LOG_FILE"

FILES=($(find "$STAGING_DIR" -type f))
pids=()

# Nombre de jobs en cours
current_jobs=0

for file in "${FILES[@]}"; do
    filename=$(basename "$file")

    # Lancement d’un traitement en arrière-plan
    process_file() {
        file="$1"
        chunk_size="$2"
        echo "▶️  Traitement de $(basename "$file")" | tee -a "$LOG_FILE"

        case "$filename" in
            api_logs_*.json)
                python3 "$PIPELINE_ROOT/processing/api_log_processor.py" --input "$file" --chunksize "$chunk_size"
                ;;
            sessions_*.csv)
                python3 "$PIPELINE_ROOT/processing/session_processor.py" --input "$file" --chunksize "$chunk_size"
                ;;
            users_database.csv)
                python3 "$PIPELINE_ROOT/processing/business_processor.py" --input "$file" --chunksize "$chunk_size"
                ;;
            products_catalog.csv|products_catalog.xlsx)
                python3 "$PIPELINE_ROOT/processing/product_processor.py" --input "$file" --chunksize "$chunk_size"
                ;;
            *)
                echo "⚠️  Type de fichier inconnu ou non pris en charge : $filename" | tee -a "$LOG_FILE"
                return
                ;;
        esac

        echo "✅ Fin de traitement pour : $filename" | tee -a "$LOG_FILE"
    }

    # Lancer en arrière-plan
    process_file "$file" "$chunk_size" &
    pids+=($!)
    ((current_jobs++))

    # Si le nombre de jobs atteint la limite, attendre avant de continuer
    if (( current_jobs >= NB_WORKERS )); then
        wait -n  # Attend qu’un seul se termine
        ((current_jobs--))
    fi
done

# Attente de tous les jobs restants
for pid in "${pids[@]}"; do
    wait "$pid"
done

echo "✅ Tous les fichiers ont été traités." | tee -a "$LOG_FILE"
