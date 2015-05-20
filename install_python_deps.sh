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

# Python 2.7
sudo apt-get install --yes \
  python-dev \
  python-pip
sudo pip install "virtualenv>1.11.1"

# GCC 4.6
# GFortran
# CMake > 2.8
sudo apt-get install --yes \
  build-essential \
  gfortran \
  cmake \
  pkg-config \
  xsltproc

# Libraries
# https://sites.google.com/a/neon-lab.com/engineering/system-setup/dependencies#TOC-Libraries
sudo apt-get install --yes \
  libatlas-base-dev \
  libyaml-0-2 \
  libmysqlclient-dev \
  libboost1.46-dbg \
  libboost1.46-dev \
  libfreetype6-dev \
  libcurl4-openssl-dev \
  libjpeg-dev \
  libsasl2-dev \
  libavcodec-dev \
  libavformat-dev \
  libavutil-dev \
  libswscale-dev \
  libtiff4-dev libjasper-dev libavformat-dev libswscale-dev libavcodec-dev libjpeg-dev libpng-dev libv4l-dev \
  fftw3-dev

. neon_repos.sh

. enable_env

echo "PRE REQUIREMENTS: "
pip install -r ${NEON_ROOT_DIR}/pre_requirements.txt --no-index --find-links ${NEON_DEPS_URL}
echo "LOCAL PACKAGES: Pyleargist linked to NumpPy"
pip install --global-option="build_ext" --global-option="--include-dirs=${VIRTUAL_ENV}/lib/python2.7/site-packages/numpy/core/include" --no-index externalLibs/pyleargist
echo "REQUIREMENTS: "
pip install -r ${NEON_ROOT_DIR}/requirements.txt --no-index --find-links ${NEON_DEPS_URL}


# vim: set ts=2 sts=2 sw=2 noexpandtab
