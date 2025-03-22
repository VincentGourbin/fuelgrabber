from requests_html import HTMLSession
import logging
import json

# Activation logs
logging.basicConfig(level=logging.INFO)

# URL cible
url_home = "https://www.prix-carburants.gouv.fr/"
url_json = "https://www.prix-carburants.gouv.fr/map/recupererOpenPdvs/"

# Lancer une session HTML (avec support JS)
session = HTMLSession()

# Étape 1 : charger la page d'accueil avec rendu JS (simule navigateur)
logging.info("Chargement de la page d'accueil...")
r = session.get(url_home)
r.html.render(timeout=20, sleep=2)  # exécute JavaScript

# Étape 2 : récupérer les cookies dynamiques
cookies = session.cookies.get_dict()
logging.info(f"Cookies récupérés : {cookies}")

# Étape 3 : utiliser les cookies pour accéder au JSON
headers = {
    'referer': url_home,
    'x-requested-with': 'XMLHttpRequest',
    'user-agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 Chrome/125 Safari/537.36',
    'accept': '*/*'
}
cookie_header = "; ".join([f"{k}={v}" for k, v in cookies.items()])
headers['cookie'] = cookie_header

logging.info("Appel à /map/recupererOpenPdvs/ avec les bons cookies...")
response = session.get(url_json, headers=headers)

print(f"Code HTTP : {response.status_code}")
if response.status_code == 200:
    try:
        data = response.json()
        print(f"✅ {len(data)} stations récupérées")
        print(json.dumps(data[0], indent=2))
    except Exception as e:
        print("Erreur parsing JSON :", e)
        print(response.text[:300])
else:
    print("Erreur HTTP :", response.status_code)
    print(response.text[:300])