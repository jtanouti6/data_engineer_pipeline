# transformations/data_enricher.py
# -*- coding: utf-8 -*-

import os
import pandas as pd
from datetime import datetime, timedelta

# ==============================
# 🔧 Utilitaires internes
# ==============================

def _ensure_datetime(series, utc=False):
    """Convertit une série en datetime (coerce), optionnel UTC."""
    s = pd.to_datetime(series, errors="coerce", utc=utc)
    # Si utc=True, on retire l'info de tz pour simplifier les calculs .dt
    return s.dt.tz_convert(None) if utc else s

def _safe_append_csv(df: pd.DataFrame, out_path: str):
    """
    Écrit un DataFrame en CSV en mode append.
    Écrit l'en-tête seulement si le fichier n'existe pas encore ou est vide.
    """
    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    write_header = True
    if os.path.exists(out_path) and os.path.getsize(out_path) > 0:
        write_header = False
    df.to_csv(out_path, index=False, mode="a", header=write_header, encoding="utf-8")

def _enriched_dir():
    """Chemin absolu vers data/processed/enriched depuis ce module."""
    return os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "data", "processed", "enriched"))

# ==============================
# 🧱 Logs API
# ==============================

def enrich_api_logs(df: pd.DataFrame, input_path: str = None) -> pd.DataFrame:
    """
    Enrichissement des logs API :
    - Catégorisation endpoint -> category
    - Extraction de la date depuis timestamp (YYYY-MM-DD)
    - Normalisation types de base
    - Export append -> data/processed/enriched/logs_enriched.csv si input_path fourni
    """
    # Normalisations de types
    if "timestamp" in df.columns:
        df["timestamp"] = _ensure_datetime(df["timestamp"])
        df["date"] = df["timestamp"].dt.date.astype("string")
    if "status_code" in df.columns:
        df["status_code"] = pd.to_numeric(df["status_code"], errors="coerce")

    # Catégorisation d'endpoint
    def _classify_endpoint(endpoint: str):
        if not isinstance(endpoint, str):
            return "other"
        if "/checkout" in endpoint:
            return "checkout"
        if "/cart" in endpoint:
            return "cart"
        if "/categories" in endpoint:
            return "catalog"
        if "/login" in endpoint or "/auth" in endpoint:
            return "auth"
        if "/products" in endpoint:
            return "product"
        return "other"

    if "endpoint" in df.columns:
        df["category"] = df["endpoint"].astype("string").apply(_classify_endpoint)

    # Export append si demandé
    if input_path:
        out = os.path.join(_enriched_dir(), "logs_enriched.csv")
        _safe_append_csv(df, out)
        print(f"💾 Données de logs enrichies (append) : {out}")

    return df

# ==============================
# 🧭 Sessions Utilisateurs
# ==============================

def enrich_session_data(df: pd.DataFrame, input_path: str = None) -> pd.DataFrame:
    """
    Enrichissement des sessions :
    - start_time, end_time -> datetime
    - duration_min
    - traffic_source (referrer)
    - device_category (device_type)
    - is_bounce, is_conversion, abandoned_cart
    - date depuis start_time
    - Export append -> data/processed/enriched/sessions_enriched.csv si input_path fourni
    """
    # Normalisations
    if "start_time" in df.columns:
        df["start_time"] = _ensure_datetime(df["start_time"])
    if "end_time" in df.columns:
        df["end_time"] = _ensure_datetime(df["end_time"])

    # Durée
    if {"start_time", "end_time"} <= set(df.columns):
        df["duration_min"] = (df["end_time"] - df["start_time"]).dt.total_seconds() / 60

    # Traffic source
    def _classify_referrer(ref):
        if not isinstance(ref, str):
            return "unknown"
        r = ref.lower()
        if "ads" in r:
            return "ads"
        if "facebook" in r or "social" in r:
            return "social"
        if "direct" in r:
            return "direct"
        if "email" in r:
            return "email"
        return "other"

    if "referrer" in df.columns:
        df["traffic_source"] = df["referrer"].apply(_classify_referrer)

    # Device category
    def _classify_device(device):
        if not isinstance(device, str):
            return "unknown"
        d = device.lower()
        if d in ("desktop", "laptop"):
            return "desktop"
        if d in ("tablet",):
            return "tablet"
        if d in ("mobile", "phone", "android", "ios"):
            return "mobile"
        return "unknown"

    if "device_type" in df.columns:
        df["device_category"] = df["device_type"].apply(_classify_device)

    # Comportements
    if "bounce_rate" in df.columns:
        df["is_bounce"] = df["bounce_rate"].astype(bool)
    if "conversion" in df.columns:
        df["is_conversion"] = df["conversion"].astype(bool)
    if {"products_added_to_cart", "conversion"} <= set(df.columns):
        df["abandoned_cart"] = (pd.to_numeric(df["products_added_to_cart"], errors="coerce").fillna(0) > 0) & (~df["conversion"].astype(bool))

    # Date
    if "start_time" in df.columns:
        df["date"] = df["start_time"].dt.date.astype("string")

    # Export append si demandé
    if input_path:
        out = os.path.join(_enriched_dir(), "sessions_enriched.csv")
        _safe_append_csv(df, out)
        print(f"💾 Données de session enrichies (append) : {out}")

    return df

# ==============================
# 📦 Produits
# ==============================

def enrich_product_data(df: pd.DataFrame, input_path: str = None) -> pd.DataFrame:
    """
    Enrichissement Produits :
    - margin, margin_pct
    - stock_status
    - is_new (créé il y a <= 30 jours)
    - date depuis created_at
    - Export append -> data/processed/enriched/products_enriched.csv si input_path fourni
    """
    # Types
    for col in ("price", "cost", "stock", "rating", "review_count"):
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    # Margin
    if {"price", "cost"} <= set(df.columns):
        df["margin"] = df["price"] - df["cost"]
        # Évite division par zéro
        df["margin_pct"] = (df["margin"] / df["cost"].where(df["cost"] != 0)).replace([pd.NA, pd.NaT], 0) * 100

    # Stock status
    def _classify_stock(x):
        try:
            v = float(x)
        except Exception:
            return "unknown"
        if v <= 10:
            return "low"
        if v <= 100:
            return "medium"
        return "high"

    if "stock" in df.columns:
        df["stock_status"] = df["stock"].apply(_classify_stock)

    # created_at & is_new
    if "created_at" in df.columns:
        df["created_at"] = _ensure_datetime(df["created_at"])
        now = datetime.now()
        df["is_new"] = (now - df["created_at"]).dt.days <= 30
        df["date"] = df["created_at"].dt.date.astype("string")

    # Export append si demandé
    if input_path:
        out = os.path.join(_enriched_dir(), "products_enriched.csv")
        _safe_append_csv(df, out)
        print(f"💾 Données de produits enrichies (append) : {out}")

    return df

# ==============================
# 👤 Utilisateurs / “Sales”
# ==============================

def enrich_user_data(df: pd.DataFrame, input_path: str = None) -> pd.DataFrame:
    """Enrichissement Utilisateurs / ventes :
    - customer_type (premium/new/returning)
    - loyalty_score
    - days_since_last_login
    - Export append -> data/processed/enriched/sales_enriched.csv si input_path fourni
    """
    # Types
    if "is_premium" in df.columns:
        df["is_premium"] = df["is_premium"].astype(bool)
    for col in ("total_orders", "total_spent", "age"):
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    if "last_login" in df.columns:
        df["last_login"] = _ensure_datetime(df["last_login"])

    # customer_type
    def _classify_customer(row):
        premium = bool(row.get("is_premium", False))
        total_orders = row.get("total_orders", 0) if pd.notna(row.get("total_orders", pd.NA)) else 0
        try:
            total_orders = int(total_orders)
        except Exception:
            total_orders = 0
        if premium:
            return "premium"
        if total_orders == 0:
            return "new"
        return "returning"

    df["customer_type"] = df.apply(_classify_customer, axis=1)

    # loyalty_score
    to = df.get("total_orders")
    ts = df.get("total_spent")
    ip = df.get("is_premium")
    if to is not None and ts is not None and ip is not None:
        df["loyalty_score"] = (
            to.fillna(0) * 1.5 +
            ts.fillna(0) * 0.05 +
            ip.astype(int) * 10
        ).round(2)

    # days_since_last_login
    if "last_login" in df.columns:
        df["days_since_last_login"] = (pd.Timestamp.now() - df["last_login"]).dt.days

    # Export append si demandé
    if input_path:
        out = os.path.join(_enriched_dir(), "sales_enriched.csv")
        _safe_append_csv(df, out)
        print(f"💾 Données de ventes enrichies (append) : {out}")

    return df
