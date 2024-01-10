import os
import requests
from bs4 import BeautifulSoup
import re

def get_links_with_years(url):
    links_with_years = []

    # Récupérer le contenu de la page
    response = requests.get(url)
    if response.status_code == 200:
        soup = BeautifulSoup(response.content, 'html.parser')

        all_links = soup.find_all('a', href=True)

        # Filtrer les liens contenant l'année 2023 de Janvier à Août
        for link in all_links:
            href = link['href']
            if re.search(r'2023-(0[1-8])', href) and 'yellow' in href.lower():
                if href.startswith('http'):
                    links_with_years.append(href)
                else:
                    base_url = '/'.join(url.split('/')[:3])
                    full_link = base_url + href
                    links_with_years.append(full_link)

    return links_with_years

# URL de la page
page_url = 'https://www1.nyc.gov/site/tlc/about/tlc-trip-record-data.page'

links_containing_years = get_links_with_years(page_url)



# Répertoire de destination des fichiers Parquet
dossier_destination = "../../data/raw" 

# Télécharger les fichiers Parquet
for lien in links_containing_years:
    nom_fichier = lien.split('/')[-1] 
    chemin_fichier = os.path.join(dossier_destination, nom_fichier)
    
    
    response = requests.get(lien)
    
  
    if response.status_code == 200:
        # Enregistrer le contenu téléchargé dans un fichier local avec le nom d'origine
        with open(chemin_fichier, 'wb') as f:
            f.write(response.content)
        print(f"Le fichier {nom_fichier} a été téléchargé et enregistré.")
    else:
        print(f"Échec du téléchargement du fichier {nom_fichier}.")


