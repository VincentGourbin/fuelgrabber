#!/usr/bin/python3
import json
import logging
import psycopg2
import requests
from datetime import datetime, timedelta
from grootgle_tools import grootgle
from alive_progress import alive_bar

# Variables à configurer
local_db = "dbname=fuel user=localuser"
local_directory_for_temp = "/home/vincent/Logs/"
yahoo_finance_api_key = "anyyahookey"

def fetch_usd_data():
    url = "https://yfapi.net/v8/finance/spark"
    querystring = {
        "symbols": "EURUSD=X",
        "interval": "1d",
        "range": "1mo"
    }
    headers = {
        'x-api-key': yahoo_finance_api_key
    }

    logging.info("Requête directe à l'API Yahoo Finance (EUR/USD)...")
    response = requests.get(url, headers=headers, params=querystring)

    if response.status_code != 200:
        raise RuntimeError(f"Échec de la requête API Yahoo (code {response.status_code}) : {response.text}")

    data = response.json()
    if "EURUSD=X" not in data:
        raise ValueError("Le symbole EURUSD=X est absent de la réponse JSON.")

    return data["EURUSD=X"]

def main():
    opt = grootgle.parse_cmdline()

    # Logger fichier + console
    logging.basicConfig(
        level=logging.DEBUG if opt.verbose else logging.INFO,
        filename=local_directory_for_temp + 'grab_usd_rate.log',
        format='%(asctime)s - %(levelname)s - %(message)s'
    )
    logging.getLogger().addHandler(logging.StreamHandler())  # Console output

    localconn = psycopg2.connect(local_db)

    try:
        stats = []

        # Récupération des données EUR/USD via API
        usd_data = fetch_usd_data()

        timestamps = usd_data.get("timestamp", [])
        closes = usd_data.get("close", [])

        if not timestamps or not closes:
            logging.error("Données de timestamp ou close manquantes.")
            return

        logging.info(f"{len(timestamps)} valeurs USD récupérées.")

        # Insertion en base
        with alive_bar(len(timestamps)) as bar:
            for ts, close in zip(timestamps, closes):
                try:
                    if close is not None:
                        date_rate = datetime.fromtimestamp(ts)
                        rate = float(close)
                        stats.append(
                            grootgle.insert_usd_rate(localconn, date_rate, rate)
                        )
                except Exception as e:
                    logging.warning(f"Erreur de traitement pour timestamp={ts}, valeur={close} : {e}")
                bar()

        # Statistiques d'insertion
        insert = sum(x.get("insert", 0) for x in stats)
        update = sum(x.get("update", 0) for x in stats)
        nothing = sum(x.get("nothing", 0) for x in stats)

        logging.info(f"Fin intégration USD : insert {insert}, update {update}, nothing {nothing}")

    except Exception as e:
        logging.error(f"Erreur lors de l'exécution : {e}")

    finally:
        localconn.close()
        logging.info("Connexion PostgreSQL fermée.")

if __name__ == "__main__":
    main()