#!/usr/bin/python3
import json
import logging
import psycopg2
import time
from datetime import datetime
from requests_html import HTMLSession
from bs4 import BeautifulSoup
from concurrent.futures import ThreadPoolExecutor, as_completed
from alive_progress import alive_bar
from grootgle_tools import grootgle

# === CONFIGURATION ===
local_db = "dbname=fuel user=localuser"
local_directory_for_temp = "/home/vincent/Logs/"

# === RÉCUPÉRATION DÉTAILS STATION ===
def fetch_station_details(session, item, headers, retries=3):
    pdv_id = item['id']
    url_detail = f"https://www.prix-carburants.gouv.fr/map/recuperer_infos_pdv/{pdv_id}"

    for attempt in range(retries):
        try:
            response_detail = session.get(url_detail, headers=headers)
            if response_detail.status_code == 200:
                soup = BeautifulSoup(response_detail.text, 'html.parser')

                station_name_tag = soup.find('h3', class_='fr-text--md')
                station_name = station_name_tag.get_text(strip=True) if station_name_tag else "N/A"

                address_tag = soup.find('p', class_='fr-text--sm')
                if address_tag:
                    for br in address_tag.find_all("br"):
                        br.replace_with("\n")
                    address_lines = address_tag.get_text("\n", strip=True).split("\n")
                    if len(address_lines) >= 2:
                        marque = address_lines[0]
                        commune = address_lines[-1].replace(item['zipCode'], '').strip()
                        adresse = "\n".join(address_lines[1:-1])
                    else:
                        marque = address_lines[0] if address_lines else "N/A"
                        adresse = "N/A"
                        commune = "N/A"
                else:
                    marque = adresse = commune = "N/A"

                return {
                    "id": pdv_id,
                    "stationName": station_name,
                    "marque": marque,
                    "adresse": adresse,
                    "commune": commune,
                    "coordLatitude": item['coordLatitude'],
                    "coordLongitude": item['coordLongitude'],
                    "deptCode": item['deptCode'],
                    "regionCode": item['regionCode'],
                    "zipCode": item['zipCode']
                }
            else:
                logging.warning(f"[{pdv_id}] Code HTTP : {response_detail.status_code}")
        except Exception as e:
            logging.warning(f"[{pdv_id}] Exception : {e}")
        time.sleep(2)
    return None

# === MAIN SCRIPT ===
def main():
    opt = grootgle.parse_cmdline()
    logging.basicConfig(
        level=logging.DEBUG if opt.verbose else logging.INFO,
        filename=local_directory_for_temp + 'grab_stations_services.log',
        format='%(asctime)s - %(levelname)s - %(message)s'
    )
    logging.getLogger().addHandler(logging.StreamHandler())

    localconn = psycopg2.connect(local_db)

    try:
        session = HTMLSession()
        url_home = "https://www.prix-carburants.gouv.fr/"
        url_json = "https://www.prix-carburants.gouv.fr/map/recupererOpenPdvs/"

        # Rendu JS pour obtenir les cookies dynamiques
        logging.info("Chargement de la page principale avec JS...")
        r = session.get(url_home)
        r.html.render(timeout=20, sleep=2)

        cookies = session.cookies.get_dict()
        logging.debug(f"Cookies récupérés : {cookies}")

        headers = {
            'referer': url_home,
            'x-requested-with': 'XMLHttpRequest',
            'user-agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 Chrome/125 Safari/537.36',
            'accept': '*/*'
        }
        headers['cookie'] = "; ".join([f"{k}={v}" for k, v in cookies.items()])

        # Requête JSON avec les bons cookies
        logging.info("Appel à /map/recupererOpenPdvs/ avec cookies...")
        response_json = session.get(url_json, headers=headers)
        if response_json.status_code != 200:
            logging.error(f"Erreur JSON HTTP {response_json.status_code}")
            return

        data = response_json.json()
        logging.info(f"{len(data)} stations à traiter.")

        # Récupération des détails
        detailed_info = []
        with alive_bar(len(data), title="Récupération détails stations") as bar:
            with ThreadPoolExecutor(max_workers=20) as executor:
                futures = {
                    executor.submit(fetch_station_details, session, item, headers): item for item in data
                }
                for future in as_completed(futures):
                    result = future.result()
                    if result:
                        detailed_info.append(result)
                    bar()

        # Insertion en base
        stats = []
        with alive_bar(len(detailed_info), title="Insertion en base") as bar_insert:
            for info in detailed_info:
                stats.append(grootgle.insert_station_service(
                    localconn,
                    info['commune'].replace("'", "''''"),
                    info['id'],
                    info['marque'].replace("'", "''''"),
                    info['stationName'].replace("'", "''''"),
                    info['deptCode'].replace("'", "''''"),
                    info['regionCode'].replace("'", "''''"),
                    info['zipCode'].replace("'", "''''"),
                    info['adresse'].replace("'", "''''"),
                    info['coordLatitude'],
                    info['coordLongitude']
                ))
                bar_insert()

        insert = sum(x.get("insert", 0) for x in stats)
        update = sum(x.get("update", 0) for x in stats)
        nothing = sum(x.get("nothing", 0) for x in stats)

        logging.info(f"Fin : insert={insert}, update={update}, nothing={nothing}")

    except Exception as e:
        logging.error(f"Erreur critique : {e}")

    finally:
        localconn.close()
        logging.info("Connexion PostgreSQL fermée.")

if __name__ == "__main__":
    main()