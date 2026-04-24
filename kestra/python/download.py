# ──────────────────────────────────────────────────────────────────────
# Download one file from TfL's website.
# Like going to the shop and buying one item from our shopping list.
# If the download fails, we try 3 times before giving up.
# ──────────────────────────────────────────────────────────────────────

import argparse
import os
import requests

parser = argparse.ArgumentParser()
parser.add_argument("--root-file", type=str, required=True)
args = parser.parse_args()


root_file = args.root_file
url = "https://cycling.data.tfl.gov.uk/usage-stats/" + root_file
OUTPUT_FILE = "../data/source/" + root_file
os.makedirs(os.path.dirname(OUTPUT_FILE), exist_ok=True)

# Download with 3 retries
session = requests.Session()
session.headers.update({
    "User-Agent": "Mozilla/5.0 (compatible; kestra-pipeline/1.0)"
})

for attempt in range(1, 4):
    try:
        with session.get(url, stream=True, timeout=300) as r:
            r.raise_for_status()
            with open(OUTPUT_FILE, "wb") as f:
                for chunk in r.iter_content(chunk_size=1 << 20):
                    f.write(chunk)
        size_mb = os.path.getsize(OUTPUT_FILE) / 1e6
        print(f"Downloaded {root_file} ({size_mb:.1f} MB)")
        break
    except Exception as e:
        print(f"Attempt {attempt}/3 for {root_file}: {e}")
        if os.path.exists(OUTPUT_FILE):
            os.remove(OUTPUT_FILE)
        if attempt == 3:
            raise
