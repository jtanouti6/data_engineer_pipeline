import os
import sys
import pandas as pd
from datetime import datetime

# =======================================
# 📁 Localisation des fichiers
# =======================================
BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
ENRICHED_DIR = os.path.join(BASE_DIR, "data", "processed", "enriched")
OUTPUT_DIR = os.path.join(BASE_DIR, "data", "processed", "joined")
os.makedirs(OUTPUT_DIR, exist_ok=True)

# ---------------------------------------
# 🔧 Helpers
# ---------------------------------------
def normalize_keys(df, keys=("user_id", "session_id")):
    for k in keys:
        if k in df.columns:
            df[k] = df[k].astype("string").str.strip()
    return df

def parse_dates_safe(df, cols):
    for c in cols:
        if c in df.columns:
            df[c] = pd.to_datetime(df[c], errors="coerce")
    return df

def read_csv_force_types(path, force_string_cols=("user_id","session_id")):
    dtype = {c: "string" for c in force_string_cols}
    return pd.read_csv(path, dtype=dtype)

# 📅 Chargement
try:
    df_users = read_csv_force_types(os.path.join(ENRICHED_DIR, "sales_enriched.csv"), ("user_id",))
    df_sessions = read_csv_force_types(os.path.join(ENRICHED_DIR, "sessions_enriched.csv"), ("user_id","session_id"))
    df_logs = read_csv_force_types(os.path.join(ENRICHED_DIR, "logs_enriched.csv"), ("user_id","session_id"))
except Exception as e:
    print(f"❌ Erreur de lecture des fichiers enrichis : {e}")
    sys.exit(1)

# 🤧 Normalisation des clés
for df in [df_users, df_sessions, df_logs]:
    df = normalize_keys(df, ("user_id", "session_id"))

# 🗓️ Parsing dates utiles
parse_dates_safe(df_users, ["registration_date", "last_login"])
parse_dates_safe(df_sessions, ["start_time", "end_time", "date"])
parse_dates_safe(df_logs, ["timestamp", "date"])

# 🪜 Déduplication
if "user_id" in df_users.columns:
    df_users = df_users.sort_values(by=["user_id","last_login"], ascending=[True, False]) \
                     .drop_duplicates(subset=["user_id"], keep="first")
if set(["session_id","user_id","timestamp"]).issubset(df_logs.columns):
    df_logs = df_logs.sort_values(by=["session_id","user_id","timestamp"], ascending=[True, True, False]) \
                     .drop_duplicates(subset=["session_id","user_id"], keep="first")
elif set(["session_id","user_id"]).issubset(df_logs.columns):
    df_logs = df_logs.drop_duplicates(subset=["session_id","user_id"], keep="first")

# 🔗 Jointures
try:
    df_merged = pd.merge(df_sessions, df_users, on="user_id", how="left")
    df_merged = pd.merge(df_merged, df_logs, on=["session_id", "user_id"], how="left", suffixes=("", "_log"))
except Exception as e:
    print(f"❌ Erreur lors des jointures : {e}")
    sys.exit(1)

# 📅 Colonnes cibles
cols = [
    "session_id","user_id","start_time","end_time","duration_seconds","pages_visited",
    "products_viewed","products_added_to_cart","conversion","total_spent_x","device_type",
    "browser","referrer","bounce_rate","country_x","city_x","duration_min","traffic_source",
    "device_category","is_bounce","is_conversion","abandoned_cart","date",
    "email","first_name","last_name","age","gender","country_y","city_y","registration_date",
    "is_premium","total_orders","total_spent_y","last_login","customer_type","loyalty_score",
    "days_since_last_login","timestamp","request_id","endpoint","method","status_code",
    "response_time_ms","user_agent","ip_address","country_code","payload_size_bytes",
    "cache_hit","error_message","category","date_log"
]
for c in cols:
    if c not in df_merged.columns:
        df_merged[c] = pd.NA

# 🔄 Export CSV final
output_path = os.path.join(OUTPUT_DIR, "combined_sessions_data.csv")
try:
    df_merged[cols].to_csv(output_path, index=False)
    print(f"✅ Fichier exporté : {output_path}")
except Exception as e:
    print(f"❌ Erreur export : {e}")
    sys.exit(1)

# 🔮 Diagnostic enrichi
def pct_missing(s):
    return round(100 * s.isna().mean(), 2)

print("\n--- Diagnostic ---")
print(f"Sessions: {len(df_sessions):,} | Users: {len(df_users):,} | Logs: {len(df_logs):,}")
users_in_sessions = set(df_sessions["user_id"].dropna())
users_in_users = set(df_users["user_id"].dropna())
keys_sessions = set(zip(df_sessions["session_id"], df_sessions["user_id"]))
keys_logs = set(zip(df_logs["session_id"], df_logs["user_id"]))

print(f"User IDs sans correspondance: {len(users_in_sessions - users_in_users)}")
print(f"Sessions sans logs: {len(keys_sessions - keys_logs)}")

for col in ["email", "first_name", "last_name", "total_spent_y", "timestamp", "endpoint"]:
    if col in df_merged.columns:
        print(f"Manquants {col}: {pct_missing(df_merged[col])}%")
