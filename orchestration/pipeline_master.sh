#!/bin/bash
# Orchestrateur principal du pipeline de données e-commerce
# Ce script pilote l'exécution des différentes étapes du traitement des données
# Définition des chemins
PIPELINE_ROOT="$(dirname "$0")/.."   # Répertoire racine du pipeline
LOG_DIR="$PIPELINE_ROOT/logs"        # Répertoire pour stocker les fichiers de log
mkdir -p "$LOG_DIR"                  # Création du dossier logs si nécessaire
LOG_FILE="$LOG_DIR/pipeline_$(date '+%Y%m%d_%H%M%S').log"  # Fichier de log horodaté


echo "📂 Répertoire racine détecté : $PIPELINE_ROOT"



# ====================================
# ✅ Verrouillage pour éviter les doublons
# ====================================

LOCK_FILE="/tmp/data_pipeline.lock"  # Fichier de verrouillage pour éviter les exécutions multiples

if [ -f "$LOCK_FILE" ]; then
    echo "❌ Pipeline déjà en cours. Fichier lock détecté." | tee -a "$LOG_FILE"
    exit 1  # Si le fichier lock existe, on quitte
fi

touch "$LOCK_FILE"  # Création du fichier lock

# Suppression automatique du lock à la fin ou en cas de crash
trap "rm -f $LOCK_FILE" EXIT

# ====================================
# 🧩 Fonctions principales du pipeline
# ====================================

initialize_data_pipeline() {
    echo "🔧 Initialisation de l'environnement..." | tee -a "$LOG_FILE"

    # Vérifie que tous les répertoires de données existent (création si nécessaire)
    for dir in raw staging processed quality archive; do
        mkdir -p "$PIPELINE_ROOT/data/$dir"
    done

    # Création du dossier de configuration s’il est manquant (utile en cas de déploiement initial)
    mkdir -p "$PIPELINE_ROOT/config"

    # Vérifie que les fichiers de configuration critiques existent, sinon avertit
    for config_file in data_schemas.json business_rules.yaml quality_thresholds.yaml pipeline_config.yaml; do
        if [ ! -f "$PIPELINE_ROOT/config/$config_file" ]; then
            echo "⚠️  Fichier de configuration manquant : $config_file" | tee -a "$LOG_FILE"
        fi
    done

    # Nettoyage optionnel du dossier staging pour éviter les conflits avec d'anciens fichiers
    if [ -n "$(ls -A "$PIPELINE_ROOT/data/staging/" 2>/dev/null)" ]; then
        echo "🧹 Nettoyage du dossier staging..." | tee -a "$LOG_FILE"
        rm -r "$PIPELINE_ROOT/data/staging/"*
    fi
    # 🔢 Détection du nombre de cœurs logiques disponibles
    CPU_CORES=$(nproc)
    MAX_WORKERS=$((CPU_CORES))  # Laisse 1 cœur libre pour la machine

    # Fallback minimum si calculé < 1
    if [[ "$MAX_WORKERS" -lt 1 ]]; then
      MAX_WORKERS=1
    fi
    yq e '.data_workers = '"$MAX_WORKERS"'' -i config/pipeline_config.yaml
    if [ $? -ne 0 ]; then
        echo "❌ Erreur : échec de mise à jour de config/pipeline_conf.yaml avec yq" | tee -a "$LOG_FILE"
        exit 1
    fi
    echo "🧮 Détection dynamique : $MAX_WORKERS workers autorisés"
    echo "🔍 Test d'existence : $PIPELINE_ROOT/config/pipeline_config.yaml"
    # Chargement des variables d'environnement depuis un fichier pipeline_config.yaml (facultatif ici)
    if [ -f "$PIPELINE_ROOT/config/pipeline_config.yaml" ]; then
    
        CONFIG_PATH="$PIPELINE_ROOT/config/pipeline_config.yaml"
        DATA_WORKERS=$(yq eval '.data_workers' "$CONFIG_PATH")
        CHUNK_SIZE_ROWS=$(yq eval '.chunk_size_rows' "$CONFIG_PATH")
        MAX_FILES=$(yq eval '.max_files' "$CONFIG_PATH")
        echo "🧪 CHUNK_SIZE_ROWS lu depuis YAML : '$CHUNK_SIZE_ROWS'"
        QUALITY_THRESHOLD=$(yq eval '.quality_threshold' "$CONFIG_PATH")
        PROCESSING_TIMEOUT=$(yq eval '.processing_timeout' "$CONFIG_PATH")
        echo "⚙️  Configuration chargée : Max_files=$MAX_FILES, Workers=$DATA_WORKERS, Chunk=$CHUNK_SIZE_ROWS Lignes, Seuil=$QUALITY_THRESHOLD%, Timeout=$PROCESSING_TIMEOUT sec" | tee -a "$LOG_FILE"
    fi

    # Affichage d’un résumé
    echo "✅ Initialisation terminée." | tee -a "$LOG_FILE"
}


scan_data_sources() {
    echo "🔎 Scan des nouvelles sources de données..." | tee -a "$LOG_FILE"
    # Appel du script dédié à la découverte des fichiers à traiter
    "$PIPELINE_ROOT/orchestration/data_discovery.sh" "$MAX_FILES" >> "$LOG_FILE" 2>&1
}

distribute_processing() {
    echo "⚙️ Lancement du traitement avec $DATA_WORKERS workers..." | tee -a "$LOG_FILE"
    # Appel du gestionnaire de traitement parallèle avec passage du nombre de workers
    "$PIPELINE_ROOT/orchestration/worker_manager.sh" "$DATA_WORKERS" "$CHUNK_SIZE_ROWS" >> "$LOG_FILE" 2>&1
}

monitor_data_quality() {
    echo "🧪 Vérification qualité..." | tee -a "$LOG_FILE"
    bash "$PIPELINE_ROOT/orchestration/quality_monitor.sh" "$QUALITY_THRESHOLD" "$DATA_WORKERS" >> "$LOG_FILE" 2>&1
}
run_alert_manager() {
    echo "📣 Analyse des alertes qualité..." | tee -a "$LOG_FILE"
    "$PIPELINE_ROOT/monitoring/alert_manager.py" >> "$LOG_FILE" 2>&1

    if [ $? -eq 0 ]; then
        echo -e "\\033[32m✅ Alerte qualité : aucun échec détecté.\\033[0m" | tee -a "$LOG_FILE"
    else
        echo -e "\\033[31m🚨 Alerte qualité : des échecs ont été détectés !\\033[0m" | tee -a "$LOG_FILE"
        echo -e "\\033[31mConsulte le fichier : data/quality/quality_alert.txt\\033[0m" | tee -a "$LOG_FILE"
    fi
}

consolidate_data_results() {
    echo "📦 consolidation des résultats multi-sources..." | tee -a "$LOG_FILE"

    # Appel du script de jointure Python
    python3  "$PIPELINE_ROOT/transformations/data_joiner.py" >> "$LOG_FILE" 2>&1

    # Vérification du résultat
    if [ $? -eq 0 ]; then
        echo "✅ consolidation terminée avec succès." | tee -a "$LOG_FILE"
    else
        echo "❌ Échec de l’agrégation des résultats." | tee -a "$LOG_FILE"
    fi
}


archive_processed_data() {
    echo "📁 Archivage complet des données..." | tee -a "$LOG_FILE"

    ARCHIVE_ROOT="$PIPELINE_ROOT/data/archive"
    TIMESTAMP=$(date +"%Y%m%d_%H%M")
    ARCHIVE_DIR="$ARCHIVE_ROOT/archive_$TIMESTAMP"
    mkdir -p "$ARCHIVE_DIR"

    # 1. Archiver les données traitées
    echo "📦 Archivage de data/processed" | tee -a "$LOG_FILE"
    cp -r "$PIPELINE_ROOT/data/processed" "$ARCHIVE_DIR/processed"

    # 2. Archiver les rapports qualité
    echo "📦 Archivage de data/quality" | tee -a "$LOG_FILE"
    cp -r "$PIPELINE_ROOT/data/quality" "$ARCHIVE_DIR/quality"

    # 3. Compression unique
    TAR_PATH="$ARCHIVE_ROOT/archive_$TIMESTAMP.tar.gz"
    tar -czf "$TAR_PATH" -C "$ARCHIVE_ROOT" "archive_$TIMESTAMP"
    echo "🗜️ Archive compressée : $TAR_PATH" | tee -a "$LOG_FILE"

    # 4. Nettoyage du dossier temporaire d'archive
    rm -rf "$ARCHIVE_DIR"
    echo "🧹 Dossier intermédiaire supprimé." | tee -a "$LOG_FILE"

    # 5. Suppression des fichiers raw et staging (sans supprimer les dossiers)
    echo "🧼 Nettoyage de data/raw et data/staging" | tee -a "$LOG_FILE"
    # find "$PIPELINE_ROOT/data/raw" -type f ! -name "*.zip" -exec rm -f {} \;
    find "$PIPELINE_ROOT/data/staging" -type f -exec rm -f {} \;
    find "$PIPELINE_ROOT/data/raw" -type f -name "*.done" -exec rm -f {} \;

    echo "✅ Archivage complet terminé." | tee -a "$LOG_FILE"
}

generate_dashboard() {
    echo "📊 Génération du dashboard HTML..." | tee -a "$LOG_FILE"
    "$PIPELINE_ROOT/monitoring/dashboard_gen.py" >> "$LOG_FILE" 2>&1

    if [ $? -eq 0 ]; then
        echo "✅ Dashboard généré avec succès." | tee -a "$LOG_FILE"
    else
        echo "❌ Erreur lors de la génération du dashboard." | tee -a "$LOG_FILE"
    fi
}


# ====================================
# 🚀 Lancement du pipeline
# ====================================

echo "🚀 DÉMARRAGE DU PIPELINE À $(date)" | tee -a "$LOG_FILE"

initialize_data_pipeline        # Étape d'initialisation => dev ok
scan_data_sources               # Détection des fichiers nouveaux => dev ok
distribute_processing           # Lancement du traitement des données => dev ok
consolidate_data_results          # (optionnel) Fusion des résultats => dev ok
monitor_data_quality            # Contrôle qualité avant-traitement => dev ok
run_alert_manager
generate_dashboard              # Génére le tableau de bord html de la qualité de donnée
# archive_processed_data          # Archivage des fichiers traités
echo "✅ PIPELINE TERMINÉ À $(date)" | tee -a "$LOG_FILE"
# 🧹 Correction des permissions pour le runner GitHub
chown -R $(id -u):$(id -g) "$PIPELINE_ROOT/data" "$PIPELINE_ROOT/logs" 2>/dev/null || true
exit 0  # Fin du script avec succès
