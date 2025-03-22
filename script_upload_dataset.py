#!/usr/bin/python3
import os
import sys
import argparse
import pandas as pd
import logging
from datetime import datetime
from dateutil.relativedelta import relativedelta
from huggingface_hub import HfApi, HfFolder
from sqlalchemy import create_engine

# === CONFIGURATION ===
DB_URI = "postgresql+psycopg2:///fuel?user=localuser"
local_directory_for_temp = "./data"
HF_USERNAME = 'VincentGOURBIN'
DATASET_NAME = 'FuelInFranceData'

# === LOGGER ===
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# === ARGPARSE ===
parser = argparse.ArgumentParser(description="Export mensuel des données carburants vers Hugging Face.")
parser.add_argument("--start", type=str, help="Date de départ au format YYYY-MM (ex: 2024-11). Par défaut : mois courant.")
args = parser.parse_args()

# === CALCUL DES DATES ===
end_date = datetime.now()
if args.start:
    try:
        start_date = datetime.strptime(args.start, "%Y-%m")
    except ValueError:
        logging.error("Format invalide pour --start. Utilise YYYY-MM.")
        sys.exit(1)
else:
    start_date = datetime(end_date.year, end_date.month, 1)

current_date = start_date

# === CONNEXION SQLALCHEMY ===
try:
    engine = create_engine(DB_URI)
    conn = engine.connect()
    logging.info("Connexion PostgreSQL via SQLAlchemy réussie.")
except Exception as e:
    logging.error("Erreur de connexion : %s", e)
    sys.exit(1)

# === AUTHENTIFICATION HUGGING FACE ===
HF_TOKEN = HfFolder.get_token()
if not HF_TOKEN:
    logging.error("Token HF manquant. Exécute 'huggingface-cli login'.")
    sys.exit(1)
else:
    logging.info("Token HF récupéré.")

api = HfApi()
full_dataset_name = f"{HF_USERNAME}/{DATASET_NAME}"
datasets_list = [ds.id for ds in api.list_datasets(author=HF_USERNAME)]

if full_dataset_name not in datasets_list:
    try:
        api.create_repo(repo_id=full_dataset_name, repo_type='dataset', token=HF_TOKEN)
        logging.info(f"Dataset Hugging Face créé : {full_dataset_name}")
    except Exception as e:
        logging.error("Erreur création dataset HF : %s", e)
        sys.exit(1)
else:
    logging.info(f"Dataset existant : {full_dataset_name}")

# === TRAITEMENT MENSUEL ===
while current_date <= end_date:
    year = current_date.year
    month = current_date.month
    logging.info(f"Traitement {year}-{month:02d}...")

    query = f'''
        SELECT a.rate_date,
               b.id AS station_id,
               b.nom,
               b.commune,
               b.marque,
               b.departement,
               b.regioncode,
               b.zipcode,
               b.address,
               b.coordlatitude,
               b.coordlongitude,
               a.fuel_name,
               a.price,
               c.brent_date,
               c.brent_rate,
               c.brent_rate_eur
        FROM fuel_price_history_data AS a
        INNER JOIN stations_services AS b ON a.id = b.id
        INNER JOIN brent_usd_eur_per_liter AS c ON DATE(a.rate_date) = DATE(c.brent_date)
        WHERE EXTRACT(YEAR FROM a.rate_date) = {year}
          AND EXTRACT(MONTH FROM a.rate_date) = {month};
    '''

    try:
        df = pd.read_sql(query, conn)
        logging.info(f"{len(df)} lignes récupérées.")
    except Exception as e:
        logging.warning(f"Erreur SQL {year}-{month:02d} : {e}")
        current_date += relativedelta(months=1)
        continue

    if df.empty:
        logging.info("Aucune donnée pour ce mois.")
        current_date += relativedelta(months=1)
        continue

    os.makedirs(local_directory_for_temp, exist_ok=True)
    csv_filename = f"{local_directory_for_temp}/dataset_{year}_{month:02d}.csv"
    df.to_csv(csv_filename, index=False, encoding='utf-8')
    logging.info(f"CSV écrit : {csv_filename}")    

    MAX_RETRIES = 3
    RETRY_DELAY = 10  # secondes

    success = False
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            with open(csv_filename, "rb") as f:
                api.upload_file(
                    path_or_fileobj=f,
                    path_in_repo=csv_filename,
                    repo_id=full_dataset_name,
                    repo_type="dataset",
                    token=HF_TOKEN,
                )
            logging.info(f"Upload Hugging Face réussi à la tentative {attempt}.")
            success = True
            break
        except Exception as e:
            logging.warning(f"Tentative {attempt}/{MAX_RETRIES} échouée : {e}")
            if attempt < MAX_RETRIES:
                logging.info(f"Nouvelle tentative dans {RETRY_DELAY} secondes...")
                time.sleep(RETRY_DELAY)
            else:
                logging.error("Échec de l'upload après plusieurs tentatives.")

    # Supprimer le fichier *seulement si* l’upload a fonctionné
    if success:
        os.remove(csv_filename)
        logging.info("Fichier local supprimé.")
    else:
        logging.warning(f"Fichier conservé pour inspection manuelle : {csv_filename}")


    current_date += relativedelta(months=1)

logging.info(f"✅ Dataset mis à jour : https://huggingface.co/datasets/{full_dataset_name}")
conn.close()