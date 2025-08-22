import os
import sys
import pandas as pd

# =======================================
# 📁 Localisation des fichiers enrichis
# =======================================
ENRICHED_DIR = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "..", "data", "processed", "enriched")
)
OUTPUT_DIR = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "..", "data", "processed", "joined")
)
os.makedirs(OUTPUT_DIR, exist_ok=True)

# =======================================
# 📥 Chargement des fichiers enrichis
# =======================================
try:
    df_users = pd.read_csv(os.path.join(ENRICHED_DIR, "sales_enriched.csv"))
    df_sessions = pd.read_csv(os.path.join(ENRICHED_DIR, "sessions_enriched.csv"))
    df_logs = pd.read_csv(os.path.join(ENRICHED_DIR, "logs_enriched.csv"))
except Exception as e:
    print(f"❌ Erreur de lecture des fichiers enrichis : {e}")
    sys.exit(1)

# =======================================
# 🔗 Jointure utilisateurs ↔ sessions
# =======================================
try:
    df_merged = pd.merge(df_sessions, df_users, on="user_id", how="left")
except Exception as e:
    print(f"❌ Erreur lors de la jointure sessions ↔ utilisateurs : {e}")
    sys.exit(1)

# =======================================
# 🔗 Jointure avec les logs API
# =======================================
try:
    df_merged = pd.merge(df_merged, df_logs, on=["session_id", "user_id"], how="left", suffixes=("", "_log"))
except Exception as e:
    print(f"❌ Erreur lors de la jointure avec les logs API : {e}")
    sys.exit(1)

# =======================================
# 💾 Export final du dataset joint
# =======================================
output_path = os.path.join(OUTPUT_DIR, "combined_sessions_data.csv")
try:
    df_merged.to_csv(output_path, index=False)
    print(f"✅ Fichier de données jointes exporté : {output_path}")
except Exception as e:
    print(f"❌ Erreur lors de l'export du fichier final : {e}")
    sys.exit(1)
