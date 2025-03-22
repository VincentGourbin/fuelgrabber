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

def fetch_brent_data():
    url = "https://yfapi.net/v8/finance/spark"
    querystring = {
        "symbols": "BZ=F",
        "interval": "1d",
        "range": "1mo"
    }
    headers = {
        'x-api-key': yahoo_finance_api_key
    }

    logging.info("Requête directe à l'API Yahoo Finance...")
    response = requests.get(url, headers=headers, params=querystring)

    if response.status_code != 200:
        raise RuntimeError(f"Échec de la requête API Yahoo (code {response.status_code}) : {response.text}")

    data = response.json()
    if "BZ=F" not in data:
        raise ValueError("Le symbole BZ=F est absent de la réponse JSON.")

    return data["BZ=F"]

def main():
    opt = grootgle.parse_cmdline()

    # Logger fichier + console
    logging.basicConfig(
        level=logging.DEBUG if opt.verbose else logging.INFO,
        filename=local_directory_for_temp + 'grab_brent_rate.log',
        format='%(asctime)s - %(levelname)s - %(message)s'
    )
    logging.getLogger().addHandler(logging.StreamHandler())  # Console output

    localconn = psycopg2.connect(local_db)

    try:
        stats = []

        # Récupération des données Brent via API
        brent_data = fetch_brent_data()

        timestamps = brent_data.get("timestamp", [])
        closes = brent_data.get("close", [])

        if not timestamps or not closes:
            logging.error("Données de timestamp ou close manquantes.")
            return

        logging.info(f"{len(timestamps)} valeurs Brent récupérées.")

        # Insertion en base
        with alive_bar(len(timestamps)) as bar:
            for ts, close in zip(timestamps, closes):
                try:
                    if close is not None:
                        date_rate = datetime.fromtimestamp(ts)
                        rate_in_usd = float(close)
                        stats.append(
                            grootgle.insert_brent_spot_price(localconn, date_rate, rate_in_usd)
                        )
                except Exception as e:
                    logging.warning(f"Erreur de traitement pour timestamp={ts}, valeur={close} : {e}")
                bar()

        # Statistiques d'insertion
        insert = sum(x.get("insert", 0) for x in stats)
        update = sum(x.get("update", 0) for x in stats)
        nothing = sum(x.get("nothing", 0) for x in stats)

        logging.info(f"Fin intégration Brent : insert {insert}, update {update}, nothing {nothing}")

    except Exception as e:
        logging.error(f"Erreur lors de l'exécution : {e}")

    finally:
        localconn.close()
        logging.info("Connexion PostgreSQL fermée.")

if __name__ == "__main__":
    main()