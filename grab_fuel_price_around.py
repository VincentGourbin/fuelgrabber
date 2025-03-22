#!/usr/bin/python3
import time
from functools import wraps
from contextlib import closing
from urllib.request import urlopen
import requests
import json
from datetime import datetime, timedelta
import logging
import psycopg2
from psycopg2.errors import SerializationFailure
from grootgle_tools import grootgle
from zipfile import ZipFile
import xmltodict
from alive_progress import alive_bar
import xml.etree.ElementTree as ET
from collections import defaultdict
from pathlib import Path

# Variables 
local_db = "dbname=fuel user=localuser"
local_directory_for_temp = "/home/vincent/Logs/"
limit_size_for_slicing = 50000000
limit_element_of_slicing = 5001
stats = []
seen_pdv = []

# Connexion locale PostgreSQL
print(f"[DEBUG] Tentative de connexion à PostgreSQL avec DSN : {local_db}")
try:
    localconn = psycopg2.connect(local_db)
    print("[DEBUG] Connexion à PostgreSQL réussie.")
except psycopg2.OperationalError as e:
    print(f"[ERREUR] Connexion à PostgreSQL échouée : {e}")
    exit(1)

# Timing wrapper pour l'insertion SQL
def timing_wrapper(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        start_time = time.perf_counter()
        result = func(*args, **kwargs)
        elapsed = (time.perf_counter() - start_time) * 1000
        if elapsed > 5:
            print(f"[SQL TIMING] {func.__name__} took {elapsed:.2f} ms")
        return result
    return wrapper

grootgle.insert_fuel_price_history_data = timing_wrapper(grootgle.insert_fuel_price_history_data)

def get_slice_of_xml_as_json(xml_file):
    with open(xml_file, 'rb') as xml_file:
        my_dict = xmltodict.parse(xml_file.read())
        json_data = json.dumps(my_dict)
    return json_data

def diet_xml_fuel_file(xml_file):
    start_tag_horaires_identified = False
    start_tag_services_identified = False
    start_tag_rupture_identified = False
    captured_line = ''
    with open(xml_file, encoding="ISO-8859-1") as f:
        for line in f:
            if "<horaires" in line:
                start_tag_horaires_identified = True
            if "<services>" in line:
                start_tag_services_identified = True
            if "<rupture" in line:
                start_tag_rupture_identified = True
            if not start_tag_horaires_identified and not start_tag_services_identified and not start_tag_rupture_identified:
                captured_line += line
            if start_tag_horaires_identified and "</horaires>" in line:
                start_tag_horaires_identified = False
            if start_tag_services_identified and "</services>" in line:
                start_tag_services_identified = False
            if start_tag_rupture_identified and "/>" in line:
                start_tag_rupture_identified = False
    return captured_line

def find_the_last_pdv(xml_file):
    last_pdv = 0
    with open(xml_file, encoding="ISO-8859-1") as f:
        for line in f:
            if "<pdv " in line:
                last_pdv = line[line.find("id=") + 4:line.find(" ", line.find("id=")) - 1]
    return last_pdv

def keep_finite_element_in_xml_file(xml_file):
    omit = False
    nb_read_elements = 0
    captured_line = ''
    end_tag_pdv_identified = False
    with open(xml_file, encoding="ISO-8859-1") as f:
        for line in f:
            if "<pdv " in line:
                current_read_id = line[line.find("id=") + 4:line.find(" ", line.find("id=")) - 1]
                end_tag_pdv_identified = False
                if current_read_id in seen_pdv:
                    omit = True
                else:
                    if nb_read_elements >= limit_element_of_slicing:
                        omit = True
                    else:
                        nb_read_elements += 1
                        seen_pdv.append(current_read_id)
            elif "</pdv>" in line:
                end_tag_pdv_identified = True
                if not omit:
                    captured_line += line
                omit = False
            else:
                end_tag_pdv_identified = False
            if not omit and not end_tag_pdv_identified:
                captured_line += line
    return captured_line

def process_json_data(decoded):
    logging.info("{} : {} info to decode and integrate in fuel history data ".format(
        datetime.now().strftime("%Y-%m-%d %H:%M:%S"), len(decoded['pdv_liste']['pdv'])))

    batch = []
    with alive_bar(len(decoded['pdv_liste']['pdv'])) as bar:
        for current in decoded['pdv_liste']['pdv']:
            if '@id' in current:
                id = current['@id']
            else:
                continue
            latitude = float(current.get('@latitude', 0)) / 100000
            longitude = float(current.get('@longitude', 0)) / 100000
            zip = current.get('@cp', '').replace("'", "''''")
            pop = current.get('@pop', '').replace("'", "''''")
            address = current.get('adresse', '').replace("'", "''''") if current.get('adresse') else None
            city = current.get('ville', '').replace("'", "''''") if current.get('ville') else None

            if latitude and longitude:
                if 'prix' in current and current['prix']:
                    for current_price in current['prix']:
                        try:
                            fuel_name = current_price['@nom']
                            fuel_rate = float(current_price['@valeur'])
                            if current_price['@valeur'].find(".") == -1:
                                fuel_rate = fuel_rate / 1000
                            try:
                                rate_date = datetime.strptime(current_price['@maj'], "%Y-%m-%dT%H:%M:%S")
                            except:
                                try:
                                    rate_date = datetime.strptime(current_price['@maj'], "%Y-%m-%d %H:%M:%S")
                                except:
                                    try:
                                        rate_date = datetime.strptime(current_price['@maj'][0:19], "%Y-%m-%d %H:%M:%S")
                                    except:
                                        continue
                            batch.append((id, latitude, longitude, zip, pop, address, city, fuel_name, fuel_rate, rate_date))
                        except:
                            continue
            bar()

    # Mode batch insert (proposition d'amélioration)
    if batch:
        print(f"[BATCH] Inserting {len(batch)} records...")
        grootgle.batch_insert_fuel_price_history_data(localconn, batch)

def main():
    opt = grootgle.parse_cmdline()
    url_for_price_daily = opt.url
    log_name = "grab_" + url_for_price_daily.replace("/", "_") + ".log"
    logging.basicConfig(level=logging.DEBUG if opt.verbose else logging.INFO, filename=local_directory_for_temp + log_name)

    try:
        print("[PERF] Téléchargement et extraction du fichier ZIP...")
        t0 = time.time()
        r = requests.get(url_for_price_daily, allow_redirects=True)
        open(local_directory_for_temp + 'url_for_price_daily.zip', 'wb').write(r.content)
        with ZipFile(local_directory_for_temp + 'url_for_price_daily.zip', 'r') as zipObject:
            for fileName in zipObject.namelist():
                if fileName.endswith('.xml'):
                    xml_file_name_for_daily_rate = fileName
                    zipObject.extract(fileName, local_directory_for_temp)
        print(f"[PERF] ZIP traité en {time.time() - t0:.2f} sec")

        t1 = time.time()
        print("[PERF] Nettoyage XML...")
        dieted_file = local_directory_for_temp + "diet_" + xml_file_name_for_daily_rate
        with open(dieted_file, "w", encoding="ISO-8859-1") as f:
            f.write(diet_xml_fuel_file(local_directory_for_temp + xml_file_name_for_daily_rate))
        print(f"[PERF] Fichier XML nettoyé en {time.time() - t1:.2f} sec")

        files_to_process = []
        if Path(dieted_file).stat().st_size > limit_size_for_slicing:
            last_pdv_file = find_the_last_pdv(dieted_file)
            current_file = 1
            while True:
                current_file_name = dieted_file + "." + str(current_file) + ".xml"
                with open(current_file_name, "w", encoding="ISO-8859-1") as f:
                    f.write(keep_finite_element_in_xml_file(dieted_file))
                files_to_process.append(current_file_name)
                if last_pdv_file in seen_pdv:
                    break
                current_file += 1
        else:
            files_to_process.append(dieted_file)

        for file_to_process in files_to_process:
            t2 = time.time()
            print(f"[PERF] Décodage JSON + insertion pour {file_to_process}")
            decoded = json.loads(get_slice_of_xml_as_json(file_to_process))
            t3 = time.time()
            print(f"[PERF] JSON chargé en {t3 - t2:.2f} sec")
            process_json_data(decoded)
            print(f"[PERF] Insertion terminée en {time.time() - t3:.2f} sec")
        
        t4 = time.time()
        print("[PERF] Insertion de données fictives...")
        grootgle.insert_fake_station_service(localconn)
        print(f"[PERF] insert_fake_station_service terminé en {time.time() - t4:.2f} sec")

        t5 = time.time()
        print("[PERF] Rafraîchissement de la vue last_rate...")
        grootgle.refresh_last_rate_view(localconn)
        print(f"[PERF] refresh_last_rate_view terminé en {time.time() - t5:.2f} sec")

        t6 = time.time()
        print("[PERF] Rafraîchissement de la vue fuel_rate_summary...")
        grootgle.refresh_fuel_rate_summary_view(localconn)
        print(f"[PERF] refresh_fuel_rate_summary_view terminé en {time.time() - t6:.2f} sec")

        t7 = time.time()
        print("[PERF] Rafraîchissement de la vue fuel_rate_ratio_brent...")
        grootgle.refresh_fuel_rate_ratio_brent_view(localconn)
        print(f"[PERF] refresh_fuel_rate_ratio_brent_view terminé en {time.time() - t7:.2f} sec")        
        
    except ValueError as ve:
        logging.debug("Erreur de traitement : %s", ve)

    localconn.close()

if __name__ == "__main__":
    main()
