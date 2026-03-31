#!/usr/bin/env bash
# startup-vm.sh — GCP VM startup script for santander-cycles-pipeline.
#
# Paste the contents of this file into "Startup script" when creating a GCP VM
# (Advanced options → Management → Automation → Startup script).
#
# What it does (runs as root at first boot):
#   - Installs: make, wget, gh CLI, Terraform, uv, bruin
#   - Clones the repo into the SSH user's home directory
#   - Runs uv sync and terraform init
#   - Creates .env from .env.example and mkdir keys/
#
# What you still do after SSH-ing in:
#   1. Place keys/terraform-sa-key.json        (manual — secret)
#   2. Edit .env  (GCP_PROJECT, LOCATION, YEARS — rest is pre-filled)
#   3. make infra-apply
#   4. make bruin-ingest

set -euo pipefail

REPO_URL="https://github.com/vytautas-peace/santander-cycles-pipeline.git"
REPO_DIR="santander-cycles-pipeline"

# ── Detect the SSH user (first non-root user with a /home dir) ────────────────
# GCP creates the user account before the startup script runs.
SSH_USER=$(getent passwd | awk -F: '$6 ~ /^\/home\// && $3 >= 1000 {print $1; exit}')
if [[ -z "$SSH_USER" ]]; then
    echo "[startup] ERROR: Could not detect SSH user. Exiting." >&2
    exit 1
fi
SSH_HOME="/home/$SSH_USER"
echo "[startup] Target user: $SSH_USER ($SSH_HOME)"

run_as_user() { sudo -u "$SSH_USER" env HOME="$SSH_HOME" PATH="$SSH_HOME/.local/bin:$SSH_HOME/.cargo/bin:/usr/local/bin:/usr/bin:/bin" "$@"; }

# ── 1. System packages ────────────────────────────────────────────────────────
echo "[startup] Updating apt..."
apt-get update -qq

apt-get install -y -qq build-essential wget gnupg software-properties-common

# ── 2. GitHub CLI ─────────────────────────────────────────────────────────────
if ! command -v gh &>/dev/null; then
    echo "[startup] Installing GitHub CLI..."
    mkdir -p -m 755 /etc/apt/keyrings
    wget -nv -O /tmp/githubcli.gpg https://cli.github.com/packages/githubcli-archive-keyring.gpg
    tee /etc/apt/keyrings/githubcli-archive-keyring.gpg < /tmp/githubcli.gpg > /dev/null
    chmod go+r /etc/apt/keyrings/githubcli-archive-keyring.gpg
    mkdir -p -m 755 /etc/apt/sources.list.d
    echo "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/githubcli-archive-keyring.gpg] https://cli.github.com/packages stable main" \
        | tee /etc/apt/sources.list.d/github-cli.list > /dev/null
    apt-get update -qq
    apt-get install -y -qq gh
fi

# ── 3. Terraform ──────────────────────────────────────────────────────────────
if ! command -v terraform &>/dev/null; then
    echo "[startup] Installing Terraform..."
    wget -qO- https://apt.releases.hashicorp.com/gpg \
        | gpg --dearmor \
        | tee /usr/share/keyrings/hashicorp-archive-keyring.gpg > /dev/null
    CODENAME=$(grep -oP '(?<=UBUNTU_CODENAME=).*' /etc/os-release 2>/dev/null || lsb_release -cs)
    echo "deb [arch=$(dpkg --print-architecture) signed-by=/usr/share/keyrings/hashicorp-archive-keyring.gpg] https://apt.releases.hashicorp.com ${CODENAME} main" \
        | tee /etc/apt/sources.list.d/hashicorp.list > /dev/null
    apt-get update -qq
    apt-get install -y -qq terraform
fi

# ── 4. uv (installed into SSH user's home) ───────────────────────────────────
if ! run_as_user bash -c 'command -v uv &>/dev/null'; then
    echo "[startup] Installing uv for $SSH_USER..."
    run_as_user bash -c 'wget -qO- https://astral.sh/uv/install.sh | sh'
fi

# ── 5. Bruin (installed into SSH user's home) ────────────────────────────────
if ! run_as_user bash -c 'command -v bruin &>/dev/null'; then
    echo "[startup] Installing Bruin for $SSH_USER..."
    run_as_user bash -c 'wget -qO- https://getbruin.com/install/cli | sh'
fi

# ── 6. Clone repo ─────────────────────────────────────────────────────────────
if [[ ! -d "$SSH_HOME/$REPO_DIR" ]]; then
    echo "[startup] Cloning repo..."
    run_as_user git clone "$REPO_URL" "$SSH_HOME/$REPO_DIR"
else
    echo "[startup] Repo already cloned, skipping."
fi

cd "$SSH_HOME/$REPO_DIR"

# ── 7. Python deps + terraform init ──────────────────────────────────────────
echo "[startup] Running uv sync..."
run_as_user bash -c "cd $SSH_HOME/$REPO_DIR && uv sync"

echo "[startup] Running terraform init..."
run_as_user bash -c "cd $SSH_HOME/$REPO_DIR/terraform && terraform init -upgrade -input=false"

# ── 8. .env + keys dir ────────────────────────────────────────────────────────
run_as_user bash -c "cp -n $SSH_HOME/$REPO_DIR/.env.example $SSH_HOME/$REPO_DIR/.env" \
    && echo "[startup] Created .env from .env.example" \
    || echo "[startup] .env already exists"

run_as_user mkdir -p "$SSH_HOME/$REPO_DIR/keys"

# ── Done ──────────────────────────────────────────────────────────────────────
cat >> "$SSH_HOME/.bashrc" <<'EOF'

# ── santander-cycles-pipeline ──────────────────────────────────────────────
cd ~/santander-cycles-pipeline 2>/dev/null && echo "Repo ready. Next steps:
  1. Place keys/terraform-sa-key.json
  2. Edit .env  (GCP_PROJECT, LOCATION, YEARS)
  3. make infra-apply
  4. make bruin-ingest"
EOF

echo "[startup] Done. VM is ready for $SSH_USER."
