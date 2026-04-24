# ────────────────────────────────────────────────────────────────────────
# Upload one parquet file to GCS, then delete the local copy.
# Overwrites the destination object if it already exists.
#
# Credentials come from GCP_SERVICE_ACCOUNT_B64 — the base64-encoded
# service-account JSON that Kestra injects from its secret store.
# ────────────────────────────────────────────────────────────────────────

import argparse
import json
import os
from pathlib import Path

from google.cloud import storage
from google.oauth2 import service_account


PARQUET_NEW = Path("/workspace/kestra/data/parquet")
BUCKET      = "san-cycles-data-pipe-bkt"
GCS_PREFIX  = "parquet"


parser = argparse.ArgumentParser()
parser.add_argument("--filename", required=True, help="Parquet filename (no path)")
args = parser.parse_args()

src = PARQUET_NEW / args.filename
if not src.exists():
    raise FileNotFoundError(src)

sa_info = os.environ.get("GCP_SERVICE_ACCOUNT")

creds   = service_account.Credentials.from_service_account_info(json.loads(sa_info))
client  = storage.Client(project=json.loads(sa_info)["project_id"], credentials=creds)

blob = client.bucket(BUCKET).blob(f"{GCS_PREFIX}/{args.filename}")
blob.upload_from_filename(str(src))
print(f"Uploaded gs://{BUCKET}/{GCS_PREFIX}/{args.filename}")

src.unlink()
print(f"Deleted → {src}")
