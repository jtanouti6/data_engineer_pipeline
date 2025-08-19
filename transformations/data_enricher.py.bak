# transformations/data_enricher.py

import pandas as pd
import os

def enrich_api_logs(df: pd.DataFrame, input_path: str = None) -> pd.DataFrame:
    """
    Enrichissement des logs API : catégorisation des endpoints + ajout date
    """
    def classify_endpoint(endpoint):
        if "/checkout" in endpoint:
            return "checkout"
        elif "/cart" in endpoint:
            return "cart"
        elif "/categories" in endpoint:
            return "catalog"
        elif "/login" in endpoint or "/auth" in endpoint:
            return "auth"
        elif "/products" in endpoint:
            return "product"
        else:
            return "other"

    df["category"] = df["endpoint"].apply(classify_endpoint)
    df["date"] = pd.to_datetime(df["timestamp"]).dt.date.astype(str)

        # Export automatique
    if input_path:
        enriched_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "data", "processed", "enriched"))
        os.makedirs(enriched_dir, exist_ok=True)
        output_file = os.path.join(enriched_dir, "logs_enriched.csv")
        df.to_csv(output_file, index=False)
        print(f"💾 Données de logs enrichies exportées vers : {output_file}")

    return df


def enrich_session_data(df: pd.DataFrame, input_path: str = None) -> pd.DataFrame:
    """
    Enrichissement des données de session :
    - Calcul de la durée de session en minutes
    - Catégorisation du trafic
    - Catégorisation du device
    - Identification des comportements utilisateurs
    """

    # Calcul de la durée de session en minutes
    df["duration_min"] = (df["end_time"] - df["start_time"]).dt.total_seconds() / 60

    # Type de trafic (referrer)
    def classify_referrer(ref):
        if pd.isna(ref):
            return "unknown"
        elif "ads" in ref:
            return "ads"
        elif "facebook" in ref or "social" in ref:
            return "social"
        elif "direct" in ref:
            return "direct"
        else:
            return "other"

    df["traffic_source"] = df["referrer"].apply(classify_referrer)

    # Catégorie de device
    def classify_device(device):
        if device == "desktop":
            return "desktop"
        elif device == "tablet":
            return "tablet"
        elif device == "mobile":
            return "mobile"
        else:
            return "unknown"

    df["device_category"] = df["device_type"].apply(classify_device)

    # Comportement utilisateur
    df["is_bounce"] = df["bounce_rate"] == True
    df["is_conversion"] = df["conversion"] == True
    df["abandoned_cart"] = (df["products_added_to_cart"] > 0) & (~df["conversion"])
    df["date"] = df["start_time"].dt.date.astype(str)
    if input_path:
        enriched_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "data", "processed", "enriched"))
        os.makedirs(enriched_dir, exist_ok=True)
        output_file = os.path.join(enriched_dir, "sessions_enriched.csv")
        df.to_csv(output_file, index=False)
        print(f"💾 Données de session enrichies exportées vers : {output_file}")
    return df
# transformations/data_enricher.py

import pandas as pd
from datetime import datetime, timedelta

def enrich_product_data(df: pd.DataFrame, input_path: str = None) -> pd.DataFrame:
    """
    Enrichissement des données produits :
    - Calcul de la marge
    - Statut de stock
    - Indication si le produit est nouveau
    """
    # 🔹 Marge brute (prix - coût)
    df["margin"] = df["price"] - df["cost"]
    df["margin_pct"] = ((df["price"] - df["cost"]) / df["cost"]) * 100

    # 🔹 Statut de stock : low, medium, high
    def classify_stock(stock):
        if stock <= 10:
            return "low"
        elif stock <= 100:
            return "medium"
        else:
            return "high"
    df["stock_status"] = df["stock"].apply(classify_stock)

    # 🔹 Produit récent : créé il y a moins de 30 jours
    now = datetime.now()
    df["is_new"] = df["created_at"].apply(lambda x: (now - x).days <= 30)
    df["created_at"] = pd.to_datetime(df["created_at"], errors="coerce")
    df["date"] = df["created_at"].dt.date.astype(str)
    if input_path:
        enriched_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "data", "processed", "enriched"))
        os.makedirs(enriched_dir, exist_ok=True)
        output_file = os.path.join(enriched_dir, "products_enriched.csv")
        df.to_csv(output_file, index=False)
        print(f"💾 Données de produits enrichies exportées vers : {output_file}")
    return df

def enrich_user_data(df: pd.DataFrame, input_path: str = None) -> pd.DataFrame:
    """
    Enrichissement des données utilisateurs :
    - Typologie client
    - Score de fidélité
    - Jours depuis dernière connexion
    """

    # Type de client
    def classify_customer(row):
        if row["is_premium"]:
            return "premium"
        elif row["total_orders"] == 0:
            return "new"
        else:
            return "returning"

    df["customer_type"] = df.apply(classify_customer, axis=1)

    # Score de fidélité
    df["loyalty_score"] = (
        df["total_orders"] * 1.5 +
        df["total_spent"] * 0.05 +
        df["is_premium"].astype(int) * 10
    ).round(2)

    # Jours depuis dernière connexion
    df["days_since_last_login"] = (
        pd.Timestamp.now() - df["last_login"]
    ).dt.days
    if input_path:
        enriched_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "data", "processed", "enriched"))
        os.makedirs(enriched_dir, exist_ok=True)
        output_file = os.path.join(enriched_dir, "sales_enriched.csv")
        df.to_csv(output_file, index=False)
        print(f"💾 Données de ventes enrichies exportées vers : {output_file}")
    return df
