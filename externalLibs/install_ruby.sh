#!/bin/bash
# --------------------------------------------
# Install Ruby 2.0 from source
# --------------------------------------------
# Install rvm
sudo apt-get install curl
curl -L https://get.rvm.io | bash -s stable
source ~/.rvm/scripts/rvm
rvm requirements

# Install Ruby
rvm install ruby 2.0.0
rvm use ruby --default

# Install RubyGems
rvm rubygems 2.1.11

# Install Ruby Dependencies
gem install fakes3
