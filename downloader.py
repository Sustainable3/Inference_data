import os
import pandas as pd
import requests
from pathlib import Path
from urllib.parse import urlparse

# Set up a proper dataset path
MICKIEWICZ_AREA = "WzgórzeMickiewicza19-24.csv"
WHOLE_AREA = "ggs19-24wew.csv"
OUTPUT_DIR = "downloaded_images"
TIMEOUT = 30

Path(OUTPUT_DIR).mkdir(exist_ok=True)

# Define the chosen dataset (MICKIEWICZ_AREA or WHOLE_AREA)
df = pd.read_csv(MICKIEWICZ_AREA)

df = df.rename(columns={"url_do_pobrania": "url"})

if "url" not in df.columns:
    raise ValueError("Column 'url' not found in the CSV file!")

urls = df["url"].dropna().unique()

print(f"There are {len(urls)} URLs to download.")

# Function to download a file from a URL
def download_file(url, output_folder):
    filename = os.path.basename(urlparse(url).path)
    if not filename:
        filename = "unnamed_file"

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

for url in urls:
    download_file(url, OUTPUT_DIR)
