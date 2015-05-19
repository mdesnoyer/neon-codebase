#!/bin/bash
cat <<EOF
# ---------------------------------
# Install GFlags
# ---------------------------------
EOF
# https://sites.google.com/a/neon-lab.com/engineering/system-setup/dependencies#TOC-GFLAGS

PS4="+GFLAGS: "
set -ex

if ! dpkg -l libgflags0 libgflags2 libgflags-dev ; then

if ! sudo dpkg -i libgflags*.deb  ; then

echo "GFlags libraries are not installed. Building."

# Install Required Packages
sudo apt-get install --yes \
 devscripts \
 debhelper

# - Install RSA Key and configure GPG Keyring (NOTE: the key value may change! the correct keyring will be reported when you attempt to acquire the package via the dget command if it's not correct)
gpg --keyserver keyserver.ubuntu.com --recv-key 8AE09345
echo 'DSCVERIFY_KEYRINGS="/etc/apt/trusted.gpg:~/.gnupg/pubring.gpg"' > ~/.devscripts

# - Acquire the Debian source for gflags (NOTE: the url may change! Refer to the URL at the end of the GFLAGS installation description to determine how to find the correct URL) 
dget https://launchpad.net/ubuntu/+archive/primary/+files/gflags_2.0-1.dsc

# - Build the package
cd gflags-2.0/
debuild -uc -us

# - The required packages are not located in the parent directory. 
cd ..
sudo dpkg -i *gflags*.deb || \
sudo apt-get install -f

# Adapted from http://askubuntu.com/questions/312173/installing-gflags-12-04
else
  echo "GFlags libraries are already installed."
fi 
fi
# vim: set ts=2 sts=2 sw=2 expandtab
