# ──────────────────────────────────────────────────────────────────────
# Open one downloaded file. If it's a zip (lunchbox), take out every
# CSV/XLSX (sandwich) into a sibling folder. If it's already a CSV,
# do nothing — it's ready to eat.
#
# Input:  source/new/FOO.zip  (or FOO.csv / FOO.xlsx)
# Output: source/new/FOO/*.csv  (zip case) — original zip deleted
# ──────────────────────────────────────────────────────────────────────

import argparse
import zipfile
from pathlib import Path

SOURCE_NEW = Path("/workspace/kestra/data/source")

parser = argparse.ArgumentParser()
parser.add_argument("--root-file", type=str, required=True)
args = parser.parse_args()

root_path = SOURCE_NEW / args.root_file

if not root_path.exists():
    raise FileNotFoundError(f"{root_path} missing — did download run?")

if root_path.suffix.lower() != ".zip":
    print(f"Not a zip ({root_path.name}) — nothing to do.")
    raise SystemExit(0)

extract_dir = SOURCE_NEW / root_path.stem
extract_dir.mkdir(parents=True, exist_ok=True)

count = 0
with zipfile.ZipFile(root_path) as zf:
    for name in zf.namelist():
        if not name.lower().endswith((".csv", ".xlsx")):
            continue
        (extract_dir / Path(name).name).write_bytes(zf.read(name))
        count += 1

print(f"Extracted {count} files from {root_path.name} → {extract_dir}")

root_path.unlink()
print(f"Deleted zip → {root_path}")
