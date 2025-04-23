import os
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin
from concurrent.futures import ThreadPoolExecutor

# URL da página onde estão os links
BASE_URL = "https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page"
DOWNLOAD_DIR = "tlc_trip_record_data"

# Cria a pasta de download se não existir
os.makedirs(DOWNLOAD_DIR, exist_ok=True)

# Faz o scraping dos links
from bs4 import BeautifulSoup
import requests
from urllib.parse import urljoin
import re
from datetime import datetime


def get_parquet_links_in_range(url, start_date_str, end_date_str):
    """
    Download only .parquet files where the name includes a date
    between start_date and end_date (format: YYYY-MM)
    """
    response = requests.get(url)
    soup = BeautifulSoup(response.text, "html.parser")

    links = []

    start_date = datetime.strptime(start_date_str, "%Y-%m")
    end_date = datetime.strptime(end_date_str, "%Y-%m")

    for a in soup.find_all("a", href=True):
        href = a["href"].strip()
        if not href.lower().endswith(".parquet"):
            continue

        # Extract YYYY-MM from filename
        match = re.search(r"(\d{4}-\d{2})", href)
        if not match:
            continue

        file_date_str = match.group(1)
        file_date = datetime.strptime(file_date_str, "%Y-%m")

        if start_date <= file_date <= end_date:
            full_url = urljoin(url, href)
            links.append(full_url)
            print(f"✅ Added: {full_url}")
        else:
            print(f"⛔ Skipped (out of range): {href}")

    return links

    return links


# Função para baixar um arquivo
def download_file(url):
    filename = os.path.join(DOWNLOAD_DIR, os.path.basename(url))
    if os.path.exists(filename):
        print(f"[SKIP] Já existe: {filename}")
        return
    print(f"[DOWNLOAD] Baixando {url}")
    with requests.get(url, stream=True) as r:
        r.raise_for_status()
        with open(filename, "wb") as f:
            for chunk in r.iter_content(chunk_size=8192):
                if chunk:  # <- prevents writing empty keep-alive chunks
                    f.write(chunk)
        r.close()  # <- Force close (optional in `with`, but helps with some edge cases)
        print(f"[OK] Salvo em {filename}")


# Função principal
def main():
    start_date_str = "2023-01"
    end_date_str = "2023-05"
    links = get_parquet_links_in_range(BASE_URL, start_date_str, end_date_str)
    print(f"Encontrados {len(links)} arquivos para baixar.")

    # Baixa os arquivos com até 5 threads simultâneas
    with ThreadPoolExecutor(max_workers=5) as executor:
        executor.map(download_file, links)


if __name__ == "__main__":
    main()
