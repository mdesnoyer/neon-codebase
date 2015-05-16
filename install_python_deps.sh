#!/bin/bash
# Installs the Neon-hosted Python packages into the virtual enviroment 
cat <<EOF
# -----------------------------------
# Install Python packages
# -----------------------------------
EOF
set -e

PS4="+PYTHON: "

NEON_ROOT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
cd "$NEON_ROOT_DIR"
. enable_env
PIP="${VIRTUAL_ENV}/bin/pip"
. neon_repos.sh

echo "PRE REQUIREMENTS: "
$PIP install -r ${NEON_ROOT_DIR}/pre_requirements.txt --no-index --find-links ${NEON_DEPS_URL}

echo "LOCAL PACKAGES: Pyleargist linked to NumpPy"
$PIP install --global-option="build_ext" --global-option="--include-dirs=${VIRTUAL_ENV}/lib/python2.7/site-packages/numpy/core/include" --no-index externalLibs/pyleargist

echo "REQUIREMETS: "
$PIP install -r ${NEON_ROOT_DIR}/requirements.txt --no-index --find-links ${NEON_DEPS_URL}
