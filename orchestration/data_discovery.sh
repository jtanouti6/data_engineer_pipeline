#!/bin/bash
# 🔍 Script de découverte et préparation des fichiers de données

PIPELINE_ROOT="$(dirname "$0")/.."
RAW_DIR="$PIPELINE_ROOT/data/raw"
STAGING_DIR="$PIPELINE_ROOT/data/staging"
LOG_FILE="$PIPELINE_ROOT/logs/pipeline.log"

mkdir -p "$STAGING_DIR" "$RAW_DIR" "$PIPELINE_ROOT/logs"

ARCHIVE_DIR="$PIPELINE_ROOT/data/archive"
TIMESTAMP=$(date +"%Y%m%d_%H%M%S")
RAW_ARCHIVE="$ARCHIVE_DIR/archive_raw_$TIMESTAMP.tar.gz"

mkdir -p "$ARCHIVE_DIR"

echo "🟡 Démarrage du scan dans $RAW_DIR" | tee -a "$LOG_FILE"

# 1. Traitement de l'archive api_logs.zip (limitée à 10 fichiers .json.gz décompressés)
API_LOGS_ZIP="$RAW_DIR/api_logs.zip"
API_LOGS_DONE="$RAW_DIR/api_logs.zip.done"
API_LOGS_TMP="$STAGING_DIR/api_logs_tmp"

if [ -f "$API_LOGS_ZIP" ] && [ ! -f "$API_LOGS_DONE" ]; then
    echo "📦 Extraction limitée de api_logs.zip (10 fichiers)" | tee -a "$LOG_FILE"
    mkdir -p "$API_LOGS_TMP" "$STAGING_DIR/api_logs"
    
    # Liste les fichiers .json.gz dans l'archive et extrait les 10 premiers
    unzip -Z1 "$API_LOGS_ZIP" '*.json.gz' | while read -r file; do
        unzip -j "$API_LOGS_ZIP" "$file" -d "$API_LOGS_TMP" >> "$LOG_FILE"
    done

    # Décompression + conversion conditionnelle en JSON Lines
    for gzfile in "$API_LOGS_TMP"/*.json.gz; do
        filename=$(basename "$gzfile" .gz)
        json_out="$STAGING_DIR/api_logs/$filename"

        # Lire les premiers caractères du fichier décompressé pour détecter le format
        first_char=$(gunzip -c "$gzfile" | head -c 1)

        if [[ "$first_char" == "[" ]]; then
            # Format JSON array → conversion nécessaire
            if gunzip -c "$gzfile" | jq -c '.[]' > "$json_out"; then
                echo "✅ Décompressé + converti JSON array → lines : $filename" | tee -a "$LOG_FILE"
            else
                echo "❌ Échec de conversion (JSON array mal formé ?) : $filename" | tee -a "$LOG_FILE"
                rm -f "$json_out"
            fi
        else
            # Format JSON lines → pas de conversion
            if gunzip -c "$gzfile" > "$json_out"; then
                echo "✅ Décompressé (JSON lines direct) : $filename" | tee -a "$LOG_FILE"
            else
                echo "❌ Échec de décompression brute : $filename" | tee -a "$LOG_FILE"
                rm -f "$json_out"
            fi
        fi
    done


    rm -rf "$API_LOGS_TMP"
    touch "$API_LOGS_DONE"
    echo "✅ api_logs.zip extrait (10 fichiers) et marqué .done" | tee -a "$LOG_FILE"
else
    echo "⏭️  api_logs.zip déjà traité ou absent" | tee -a "$LOG_FILE"
fi


# 2. Sessions utilisateur (user_sessions/*.csv)
SESSION_SRC="$RAW_DIR/user_sessions"
SESSION_DEST="$STAGING_DIR/user_sessions"
mkdir -p "$SESSION_DEST"

if [ -d "$SESSION_SRC" ]; then
    find "$SESSION_SRC" -maxdepth 1 -type f -name "sessions_*.csv" | while read -r file; do
        filename=$(basename "$file")
        done_marker="$SESSION_SRC/$filename.done"

        if [ ! -f "$done_marker" ]; then
            cp "$file" "$SESSION_DEST/$filename"
            touch "$done_marker"
            echo "📥 Session copiée : $filename" | tee -a "$LOG_FILE"
        else
            echo "⏭️  Session déjà traitée : $filename" | tee -a "$LOG_FILE"
        fi
    done
else
    echo "⚠️  Dossier user_sessions introuvable" | tee -a "$LOG_FILE"
fi


# 3. Fichiers de données de vente : produits + commandes
mkdir -p "$STAGING_DIR/sales_data"
for sales_file in "products_catalog.csv" "products_catalog.xlsx" "users_database.csv"; do
    src_file="$RAW_DIR/$sales_file"
    done_marker="$RAW_DIR/$sales_file.done"
    if [ -f "$src_file" ] && [ ! -f "$done_marker" ]; then
        cp "$src_file" "$STAGING_DIR/sales_data/$sales_file"
        touch "$done_marker"
        echo "📥 Fichier ventes copié : $sales_file" | tee -a "$LOG_FILE"
    else
        echo "⏭️  Fichier déjà traité ou absent : $sales_file" | tee -a "$LOG_FILE"
    fi
done

echo "✅ Scan terminé." | tee -a "$LOG_FILE"


# # 4. Archivage des fichiers RAW nouvellement traités
# echo "📦 Archivage des fichiers RAW traités..." | tee -a "$LOG_FILE"

# files_to_archive=$(find "$RAW_DIR" -maxdepth 1 -type f \( -name "*.csv" -o -name "*.xlsx" -o -name "*.zip" \) -exec test -f "{}.done" \; -print)

# if [ -n "$files_to_archive" ]; then
#     tar -czf "$RAW_ARCHIVE" $files_to_archive
#     echo "✅ Archive créée : $RAW_ARCHIVE" | tee -a "$LOG_FILE"
# else
#     echo "ℹ️ Aucun fichier à archiver pour le moment." | tee -a "$LOG_FILE"
# fi