import os
import pandas as pd
import requests
from pathlib import Path
from urllib.parse import urlparse

MICKIEWICZ_AREA = "WzgorzeMickiewicza_1924.csv"
WHOLE_AREA = "ggs19-24wew.csv"

OUTPUT_DIR = "downloaded_images"
TIMEOUT = 100

Path(OUTPUT_DIR).mkdir(exist_ok=True)

df = pd.read_csv(MICKIEWICZ_AREA)

df = df.rename(columns={
    "url_do_pobrania": "url",
    "akt_rok": "rok"
})

if not {"url", "rok"}.issubset(df.columns):
    raise ValueError("Columns 'url' and/or 'rok' not found in the CSV file!")

df = df.dropna(subset=["url", "rok"])

print(f"There are {len(df)} files to download.")

def download_file(url, rok, output_folder):
    base_name = os.path.basename(urlparse(url).path)

    if not base_name:
        base_name = "unnamed_file"

    filename = f"{int(rok)}_{base_name}"
    output_path = Path(output_folder) / filename

    try:
        print(f"Downloading: {url}")
        response = requests.get(url, timeout=TIMEOUT)
        response.raise_for_status()

        with open(output_path, "wb") as f:
            f.write(response.content)

        print(f"Saved: {output_path}")

    except Exception as e:
        print(f"Error downloading {url}: {e}")

for _, row in df.iterrows():
    download_file(row["url"], row["rok"], OUTPUT_DIR)
