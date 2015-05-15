#!/bin/bash
cat <<EOF
# -----------------------------------------
# Hadoop (EMR) libraries and packages
# -----------------------------------------
EOF

PS4="+HADOOP: "
set -ex

lsb_codename=$(lsb_release --codename --short)

# Add the cdh5 apt repo 
curl  -s http://archive.cloudera.com/cdh5/ubuntu/${lsb_codename}/amd64/cdh/cloudera.list | sudo tee /etc/apt/sources.list.d/cloudera.list

# Then add the key and update:
curl -s http://archive.cloudera.com/cdh5/ubuntu/${lsb_codename}/amd64/cdh/archive.key | sudo apt-key add -

# Install the java ppa
# http://www.webupd8.org/2012/01/install-oracle-java-jdk-7-in-ubuntu-via.html
echo yes |sudo add-apt-repository --yes ppa:webupd8team/java
#oracle-java7-set-default

# Update Apt
sudo apt-get update

# Then install the stuff we use:
echo oracle-java7-installer shared/accepted-oracle-license-v1-1 select true | sudo /usr/bin/debconf-set-selections
sudo apt-get install --yes \
  oracle-java7-installer \
  eclipse \
  hadoop-mapreduce \
  avro-tools

# vim: set ts=2 sts=2 tw=2 expandtab
