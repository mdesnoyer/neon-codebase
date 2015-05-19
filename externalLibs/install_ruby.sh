#!/bin/bash
cat <<EOF
# --------------------------------------------
# Install Ruby 2.0 from source
# --------------------------------------------
EOF

PS4="+RUBY: "
#set -e

# Install rvm
sudo apt-get install --yes curl
gpg --keyserver hkp://keys.gnupg.net --recv-keys 409B6B1796C275462A1703113804BB82D39DC0E3
curl -L https://get.rvm.io | bash -s stable
source ~/.bash_profile
source ~/.rvm/scripts/rvm
rvm requirements

# Install Ruby
if ! rvm current |grep ruby-2.0.0 ; then
  rvm install ruby 2.0.0
  rvm use ruby --default
fi

# Install RubyGems
rvm rubygems 2.1.11

# Install Ruby Dependencies
gem install fakes3
