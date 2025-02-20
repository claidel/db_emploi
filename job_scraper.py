import os
import json
import threading
import requests
from datetime import datetime
from flask import Flask, jsonify
from bs4 import BeautifulSoup
from pymongo import MongoClient

try:
    import offreBot  # Assurez-vous que ce fichier existe dans le projet
except ImportError:
    print("❌ Erreur : le fichier 'offreBot.py' est introuvable !")
    exit(1)

# Vérification des variables essentielles
MONGO_URI = getattr(offreBot, "MONGO_URI", None)
MISTRAL_API_KEY = getattr(offreBot, "MISTRAL_API_KEY", None)

if not MONGO_URI:
    print("❌ Erreur : MONGO_URI non défini dans offreBot.py")
    exit(1)

if not MISTRAL_API_KEY:
    print("❌ Erreur : MISTRAL_API_KEY non défini dans offreBot.py")
    exit(1)

app = Flask(__name__)

class JobScraper:
    """Scraper pour récupérer les offres d'emploi et les stocker dans MongoDB."""

    def __init__(self, url, mongo_uri, db_name, collection_name):
        self.url = url
        self.headers = {"User-Agent": "Mozilla/5.0"}
        self.client = MongoClient(mongo_uri, tls=True, tlsAllowInvalidCertificates=True)
        self.db = self.client[db_name]
        self.collection = self.db[collection_name]

        # Vérification connexion MongoDB
        try:
            self.client.server_info()
            print("✅ Connexion réussie à MongoDB")
        except Exception as e:
            print(f"❌ Erreur de connexion à MongoDB : {e}")
            exit(1)

    def fetch_html(self):
        """Récupère le HTML de la page web."""
        try:
            response = requests.get(self.url, headers=self.headers)
            response.raise_for_status()
            return response.text
        except requests.RequestException as e:
            print(f"❌ Erreur lors de la récupération de la page : {e}")
            return None

    def extract_jobs_from_html(self, html):
        """Extrait les offres d'emploi avec BeautifulSoup."""
        soup = BeautifulSoup(html, "html.parser")
        jobs = []

        for cols3_div in soup.find_all("div", class_="Cols3"):
            for job_card in cols3_div.find_all("div", class_="Cols3_item"):
                title_element = job_card.find("p")
                company_location = job_card.find("a").text.strip().split("\n")

                title = title_element.text.strip() if title_element else "N/A"
                company = company_location[0].strip() if len(company_location) > 0 else "N/A"
                location = company_location[1].strip() if len(company_location) > 1 else "N/A"
                link_element = job_card.find("a")
                link = "https://www.mediacongo.net/" + link_element["href"] if link_element else "N/A"

                jobs.append({
                    "title": title,
                    "company": company,
                    "location": location,
                    "url": link
                })
        return jobs

    def extract_full_text(self, url):
        """Récupère et nettoie tout le texte d'une offre d'emploi."""
        try:
            response = requests.get(url, headers=self.headers)
            response.raise_for_status()
            soup = BeautifulSoup(response.text, "html.parser")

            for tag in soup(["script", "style", "noscript", "iframe", "meta", "header", "footer"]):
                tag.extract()

            return soup.get_text(separator="\n", strip=True)
        except requests.RequestException as e:
            print(f"❌ Erreur lors de l'extraction du texte : {e}")
            return None

    def summarize_with_mistral(self, text):
        """Appelle l'API de Mistral pour résumer l'offre d'emploi."""
        try:
            response = requests.post(
                url="https://openrouter.ai/api/v1/chat/completions",
                headers={
                    "Authorization": f"Bearer {MISTRAL_API_KEY}",
                    "Content-Type": "application/json",
                },
                data=json.dumps({
                    "model": "mistralai/mistral-small-24b-instruct-2501:free",
                    "messages": [{"role": "user", "content": f'{offreBot.SCRIPT} \n {text}'}]
                })
            )

            response_data = response.json()

            if "choices" not in response_data:
                print("❌ Erreur: L'API Mistral ne contient pas 'choices'.")
                return None

            return response_data['choices'][0]['message']['content']
        except requests.exceptions.JSONDecodeError:
            print("❌ Erreur: L'API Mistral a renvoyé un JSON invalide.")
            return None
        except Exception as e:
            print(f"❌ Erreur inattendue lors de l'appel à Mistral : {e}")
            return None

    def run_scraper(self):
        """Exécute le scraping et stocke les données en base."""
        html_content = self.fetch_html()
        if not html_content:
            print("❌ Échec de la récupération du contenu HTML.")
            return

        job_list = self.extract_jobs_from_html(html_content)
        if not job_list:
            print("❌ Aucune offre trouvée.")
            return

        for job in job_list:
            job_url = job['url']
            print(f"📌 Vérification de l'offre : {job_url}")

            if self.collection.find_one({"url": job_url}):
                print("⚠️ Offre déjà existante dans la base de données. Ignorée.\n")
                continue  

            job_text = self.extract_full_text(job_url)
            if not job_text:
                print(f"❌ Impossible d'extraire le texte de l'offre : {job_url}")
                continue  

            resumeAI = self.summarize_with_mistral(job_text)
            if resumeAI is None:
                print(f"❌ L'API Mistral a échoué, l'offre ne sera pas enregistrée : {job_url}\n")
                continue  

            job_entry = {
                "title": job["title"],
                "company": job["company"],
                "location": job["location"],
                "url": job_url,
                "resume": resumeAI,
                "created_at": datetime.utcnow()
            }

            try:
                result = self.collection.insert_one(job_entry)
                print(f"✅ Offre enregistrée : {job['title']} (ID: {result.inserted_id})\n")
            except Exception as e:
                print(f"❌ Erreur lors de l'enregistrement dans MongoDB : {e}\n")

@app.route("/")
def home():
    return jsonify({"message": "Job Scraper is running!"})

@app.route("/scrape")
def scrape():
    threading.Thread(target=scraper.run_scraper).start()
    return jsonify({"message": "Scraping started!"})

if __name__ == "__main__":
    scraper = JobScraper(
        url="https://www.mediacongo.net/emplois/",
        mongo_uri=MONGO_URI,
        db_name="job_database",
        collection_name="jobs"
    )
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port, debug=True)
