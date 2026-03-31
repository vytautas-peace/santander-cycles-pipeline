#!/usr/bin/env bash
# setup-vm.sh — Install all dependencies for santander-cycles-pipeline on a fresh Debian/Ubuntu VM.
#
# USAGE
#   1. gh auth login          (interactive — do this first)
#   2. gh repo clone vytautas-peace/santander-cycles-pipeline
#   3. cd santander-cycles-pipeline
#   4. bash scripts/setup-vm.sh
#   5. Restart your terminal (PATH updates need a fresh shell)
#   6. Place keys/terraform-sa-key.json  (manual — secret)
#   7. Edit .env  (GCP_PROJECT, LOCATION, YEARS — rest is pre-filled)
#   8. make infra-apply
#   9. make bruin-ingest

set -euo pipefail

info()  { echo "[setup] $*"; }
check() { command -v "$1" &>/dev/null && info "$1 already installed, skipping" && return 0 || return 1; }

# ── 1. System packages ────────────────────────────────────────────────────────
info "Updating apt..."
sudo apt-get update -qq

# make / build tools
if ! check make; then
    info "Installing build-essential (make, gcc, ...)..."
    sudo apt-get install -y -qq build-essential
fi

# wget (needed for several installers below)
if ! check wget; then
    sudo apt-get install -y -qq wget
fi

# ── 2. GitHub CLI ─────────────────────────────────────────────────────────────
if ! check gh; then
    info "Installing GitHub CLI..."
    sudo mkdir -p -m 755 /etc/apt/keyrings
    wget -nv -O /tmp/githubcli.gpg https://cli.github.com/packages/githubcli-archive-keyring.gpg
    sudo tee /etc/apt/keyrings/githubcli-archive-keyring.gpg < /tmp/githubcli.gpg > /dev/null
    sudo chmod go+r /etc/apt/keyrings/githubcli-archive-keyring.gpg
    sudo mkdir -p -m 755 /etc/apt/sources.list.d
    echo "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/githubcli-archive-keyring.gpg] https://cli.github.com/packages stable main" \
        | sudo tee /etc/apt/sources.list.d/github-cli.list > /dev/null
    sudo apt-get update -qq
    sudo apt-get install -y -qq gh
fi

# ── 3. uv (Python package manager) ───────────────────────────────────────────
if ! check uv; then
    info "Installing uv..."
    wget -qO- https://astral.sh/uv/install.sh | sh
    # uv installs to ~/.cargo/bin or ~/.local/bin — source env so it's on PATH now
    export PATH="$HOME/.local/bin:$HOME/.cargo/bin:$PATH"
fi

# ── 4. Terraform ──────────────────────────────────────────────────────────────
if ! check terraform; then
    info "Installing Terraform..."
    sudo apt-get install -y -qq gnupg software-properties-common
    wget -qO- https://apt.releases.hashicorp.com/gpg \
        | gpg --dearmor \
        | sudo tee /usr/share/keyrings/hashicorp-archive-keyring.gpg > /dev/null
    CODENAME=$(grep -oP '(?<=UBUNTU_CODENAME=).*' /etc/os-release 2>/dev/null || lsb_release -cs)
    echo "deb [arch=$(dpkg --print-architecture) signed-by=/usr/share/keyrings/hashicorp-archive-keyring.gpg] https://apt.releases.hashicorp.com ${CODENAME} main" \
        | sudo tee /etc/apt/sources.list.d/hashicorp.list > /dev/null
    sudo apt-get update -qq
    sudo apt-get install -y -qq terraform
fi

# ── 5. Bruin ──────────────────────────────────────────────────────────────────
if ! check bruin; then
    info "Installing Bruin..."
    wget -qO- https://getbruin.com/install/cli | sh
    export PATH="$HOME/.local/bin:$HOME/.bruin/bin:$PATH"
fi

# ── 6. Project setup ──────────────────────────────────────────────────────────
info "Installing Python dependencies (uv sync)..."
uv sync

info "Initialising Terraform..."
(cd terraform && terraform init -upgrade -input=false)

info "Creating .env from .env.example (if not already present)..."
cp -n .env.example .env && echo "  Created .env — update GCP_PROJECT, LOCATION, YEARS" \
    || echo "  .env already exists"

info "Creating keys/ directory..."
mkdir -p keys

# ── 7. Optional: Claude Code ──────────────────────────────────────────────────
if ! check claude; then
    read -r -p "[setup] Install Claude Code? [y/N] " ans
    if [[ "${ans,,}" == "y" ]]; then
        curl -fsSL https://claude.ai/install.sh | bash
    fi
fi

# ── Done ──────────────────────────────────────────────────────────────────────
echo ""
echo "============================================================"
echo " Setup complete. Restart your terminal, then:"
echo ""
echo "  1. Copy keys/terraform-sa-key.json  (manual — secret)"
echo "  2. Edit .env  (GCP_PROJECT, LOCATION, YEARS)"
echo "  3. make infra-apply"
echo "  4. make bruin-ingest"
echo "============================================================"
