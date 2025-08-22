#!/usr/bin/env python3
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
def normalize_keys(df: pd.DataFrame, keys=("user_id", "session_id")) -> pd.DataFrame:
    """Cast en string + strip pour les clés de jointure."""
    for k in keys:
        if k in df.columns:
            df[k] = df[k].astype("string").str.strip()
    return df

def parse_dates_safe(df: pd.DataFrame, cols) -> pd.DataFrame:
    """to_datetime(errors='coerce') pour les colonnes de dates."""
    for c in cols:
        if c in df.columns:
            df[c] = pd.to_datetime(df[c], errors="coerce")
    return df

def read_csv_force_types(path: str, force_string_cols=("user_id","session_id")) -> pd.DataFrame:
    """Lecture robuste avec dtype forcé et low_memory=False."""
    dtype = {c: "string" for c in force_string_cols}
    return pd.read_csv(path, dtype=dtype, low_memory=False)

def safe_load(path: str, force_cols=("user_id","session_id")) -> pd.DataFrame:
    if not os.path.exists(path):
        print(f"⚠️  Fichier manquant (skip) : {path}")
        return pd.DataFrame()
    try:
        df = read_csv_force_types(path, force_cols)
        return df
    except Exception as e:
        print(f"❌ Erreur de lecture {path}: {e}")
        return pd.DataFrame()

# =======================================
# 📅 Chargement
# =======================================
users_path    = os.path.join(ENRICHED_DIR, "sales_enriched.csv")
sessions_path = os.path.join(ENRICHED_DIR, "sessions_enriched.csv")
logs_path     = os.path.join(ENRICHED_DIR, "logs_enriched.csv")

df_users    = safe_load(users_path,    ("user_id",))
df_sessions = safe_load(sessions_path, ("user_id","session_id"))
df_logs     = safe_load(logs_path,     ("user_id","session_id"))

# Si sessions vides => rien à faire
if df_sessions.empty:
    print("⚠️ sessions_enriched.csv est vide ou absent — arrêt.")
    sys.exit(0)

# 🤧 Normalisation des clés (⚠️ réassignation explicite)
df_users    = normalize_keys(df_users,    ("user_id",))
df_sessions = normalize_keys(df_sessions, ("user_id","session_id"))
df_logs     = normalize_keys(df_logs,     ("user_id","session_id"))

# 🗓️ Parsing dates utiles
df_users    = parse_dates_safe(df_users,    ["registration_date", "last_login"])
df_sessions = parse_dates_safe(df_sessions, ["start_time", "end_time", "date"])
df_logs     = parse_dates_safe(df_logs,     ["timestamp", "date"])

# 🪜 Déduplication (avec garde-fous)
if "user_id" in df_users.columns and not df_users.empty:
    sort_cols = [c for c in ["user_id","last_login"] if c in df_users.columns]
    if sort_cols:
        df_users = df_users.sort_values(by=sort_cols, ascending=[True] + [False]*(len(sort_cols)-1))
    df_users = df_users.drop_duplicates(subset=["user_id"], keep="first")

if {"session_id","user_id"} <= set(df_logs.columns) and not df_logs.empty:
    sort_cols = [c for c in ["session_id","user_id","timestamp"] if c in df_logs.columns]
    if sort_cols:
        # session_id asc, user_id asc, timestamp desc si présent
        ascending = [True, True] + ([False] if "timestamp" in sort_cols else [])
        # Ajustement longueur ascending = longueur sort_cols
        if len(ascending) != len(sort_cols):
            ascending = [True]*len(sort_cols)
        df_logs = df_logs.sort_values(by=sort_cols, ascending=ascending)
    df_logs = df_logs.drop_duplicates(subset=["session_id","user_id"], keep="first")

# 🔗 Jointures (partir des sessions)
try:
    # join users (m:1)
    if "user_id" in df_users.columns:
        df_merged = pd.merge(df_sessions, df_users, on="user_id", how="left")
    else:
        print("⚠️  'user_id' manquant dans users — join users ignorée.")
        df_merged = df_sessions.copy()

    # join logs (1:1) sur (session_id,user_id) ; suffixe _log pour colonnes logs
    if {"session_id","user_id"} <= set(df_logs.columns):
        df_merged = pd.merge(
            df_merged, df_logs,
            on=["session_id", "user_id"],
            how="left",
            suffixes=("", "_log")
        )
    else:
        print("⚠️  (session_id,user_id) manquants dans logs — join logs ignorée.")
except Exception as e:
    print(f"❌ Erreur lors des jointures : {e}")
    sys.exit(1)

# 📅 Colonnes cibles (crée celles manquantes pour stabiliser le schéma)
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
# map vers les colonnes effectives après suffixe logs
if "date_log" not in df_merged.columns and "date_log" not in cols:
    cols.append("date_log")
if "date_log" not in df_merged.columns and "date" in df_merged.columns:
    # après le merge avec suffixes=("", "_log"), la date des logs s’appelle normalement 'date_log'
    # si ce n’est pas le cas (colonnes différentes), on ne touche pas ici
    pass

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

if "user_id" in df_sessions.columns and "user_id" in df_users.columns:
    users_in_sessions = set(df_sessions["user_id"].dropna())
    users_in_users = set(df_users["user_id"].dropna())
    print(f"User IDs sans correspondance: {len(users_in_sessions - users_in_users)}")

if {"session_id","user_id"} <= set(df_sessions.columns) and {"session_id","user_id"} <= set(df_logs.columns):
    keys_sessions = set(zip(df_sessions["session_id"], df_sessions["user_id"]))
    keys_logs = set(zip(df_logs["session_id"], df_logs["user_id"]))
    print(f"Sessions sans logs: {len(keys_sessions - keys_logs)}")

for col in ["email", "first_name", "last_name", "total_spent_y", "timestamp", "endpoint"]:
    if col in df_merged.columns:
        print(f"Manquants {col}: {pct_missing(df_merged[col])}%")
