# 🚀 Data Pipeline E-commerce

## 📌 Description
Ce projet met en place un **pipeline de données modulaire et industrialisé** pour analyser les performances d’un site e-commerce.  
Il gère l’ingestion, la validation qualité, le traitement, l’enrichissement, l’agrégation et l’archivage de plusieurs sources hétérogènes.

## 📂 Sources de données
- **Logs API** : fichiers `.json.gz` regroupés dans une archive `api_logs.zip`
- **Sessions utilisateurs** : fichiers `.csv`
- **Produits** : fichiers `.csv` et `.xlsx`
- **Utilisateurs** : fichiers `.csv` (base clients, premium, etc.)
- **Ventes / business metrics** : fichiers `.csv`

## 🏗️ Architecture
```
data/
 ├── raw/             # Données brutes (archives, dépôts initiaux)
 ├── staging/         # Données intermédiaires (après extraction/décompression)
 ├── processed/       # Données traitées (enrichies, agrégées, jointes)
 │    ├── api_logs/
 │    ├── sessions/
 │    ├── products/
 │    ├── sales/
 │    ├── users/
 │    ├── enriched/
 │    └── joined/
 ├── quality/         # Rapports de validation + dashboard qualité
 └── archive/         # Archives compressées datées
logs/                 # Journaux d’exécution
config/               # Paramètres YAML/JSON (schemas, règles qualité)
orchestration/        # Scripts Bash (master + workers)
processing/           # Scripts Python de traitement (1 par source)
transformations/      # Modules Python (cleaner, enricher, aggregator, formatter)
```

## ⚙️ Fonctionnalités principales

### 1. **Orchestration Bash**
- `pipeline_master.sh` : pilote global  
- Détection dynamique du nombre de CPU et des **workers en parallèle**  
- Lecture de la configuration via `config/pipeline_config.yaml`

### 2. **Parallélisme**
- `worker_manager.sh` : exécute les traitements Python en parallèle  
- Nombre de workers ajusté automatiquement en fonction du CPU (`nproc - 1`)  
- Chaque worker traite un fichier indépendant (logs, sessions, produits, …)

### 3. **Traitement par morceaux (chunking)**
- Paramètre `chunksize` transmis aux scripts Python  
- Traitement des gros fichiers CSV/JSON par itération (`100 000 lignes` par défaut)  
- Permet d’éviter une surcharge mémoire et d’accélérer le flux

### 4. **Validation qualité**
- `data_validator.py` : vérifie
  - Complétude (taux de valeurs manquantes)
  - Respect du schéma (`data_schemas.json`)
  - Règles métier (`business_rules.yaml`)
  - Seuil de qualité global (`quality_thresholds.yaml`)
- `quality_monitor.sh` : parallélise la validation sur plusieurs fichiers
- Génération de rapports JSON par fichier

### 5. **Monitoring & alerting**
- `dashboard_gen.py` : génère un **dashboard HTML** synthétique  
- `alert_manager.py` : déclenche une alerte (simulée email) si échec qualité  
- Résultats sauvegardés dans `data/quality/`

### 6. **Archivage**
- `archive_processed_data` dans `pipeline_master.sh` :
  - Archive les fichiers traités (`processed`, `raw`, `staging`, `quality`)
  - Nommage daté et compressé `.tar.gz`
  - Nettoyage des répertoires après traitement

### 7. **Dockerisation**
- Image Docker avec :
  - Python 3.11 + pandas
  - Bash, jq, yq, unzip, htop, parallel
- `docker-compose.yml` pour monter `data/` et `logs/` en volumes
- User mapping (`UID:GID`) pour gérer les droits

### 8. **CI/CD (GitHub Actions)**
- Workflow `docker-image.yml` :
  - Build + versioning de l’image Docker
  - Lancement du pipeline en runner self-hosted
  - Sauvegarde des artefacts (logs + dashboard qualité)
  - Correction automatique des permissions

## 🧪 Exemple d’exécution
```bash
# Lancer le pipeline complet
bash orchestration/pipeline_master.sh
```

Sortie typique :
```
🔧 Initialisation de l'environnement...
🧮 Détection dynamique : 6 workers autorisés
⚙️  Lancement du traitement avec 6 workers...
✅ Traitement des sessions terminé.
✅ Traitement des produits terminé.
✅ api_logs.zip traité et marqué .done
📝 Rapport sauvegardé : validation_report_sessions_20250718.csv.json
📊 Dashboard HTML généré : data/quality/dashboard.html
📩 Alerte générée : quality_alert.txt
🗜️ Archive compressée : data/archive/archive_20250818_1334.tar.gz
```

## 🚀 Performances observées
- **Volume testé** : ~5 Go de données brutes  
- **Temps total** : ~41 minutes (avec 6 workers CPU et chunking activé)  
- **Optimisations implémentées** :
  - Parallélisme CPU (N-1 cœurs logiques utilisés)  
  - Lecture par morceaux (`chunksize` dynamique)  
  - Détection automatique du format JSON (array vs lines)  

---

👉 À ce stade, le pipeline est **modulaire, extensible et industrialisé**.  
La prochaine étape (optionnelle) est l’intégration **Big Data (HDFS / PySpark / Hive / Delta)** pour passer à l’échelle.
