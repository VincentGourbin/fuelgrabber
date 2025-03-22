#!/bin/bash

# === CHEMINS ===
VENV_PATH="/home/vincent/Scripts/fuel/venvfuel"
SCRIPT_PATH="/home/vincent/Scripts/fuel"
BACKUP_SCRIPT="/home/vincent/Scripts/make_backup.sh"

# === ACTIVER L'ENV PYTHON ===
source "$VENV_PATH/bin/activate"

# === EXÉCUTION DES SCRIPTS ===
python3 "$SCRIPT_PATH/grab_brent_rate.py" ""
python3 "$SCRIPT_PATH/grab_usd_rate.py" ""
python3 "$SCRIPT_PATH/grab_fuel_price_around.py" "" -u "https://donnees.roulez-eco.fr/opendata/instantane"
python3 "$SCRIPT_PATH/grab_stations_services.py" ""

# === SAUVEGARDE ===
bash "$BACKUP_SCRIPT"