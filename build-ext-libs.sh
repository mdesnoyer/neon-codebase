
#!/bin/bash
# -----------------------------------------------------------
# Build the Neon External Libraries and their dependencies in the proper order
# -----------------------------------------------------------
# https://sites.google.com/a/neon-lab.com/engineering/system-setup/dependencies
#
set -e

NEON_ROOT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
cd "$NEON_ROOT_DIR"

case $(uname -s) in                                                                                                                                                                    ( Darwin )
    echo "ERROR: OSX is not supported." 1>&2
    exit 1
    ;;
  ( Linux )
    # Presumed to be Ubuntu
    lsb_rel=$(lsb_release --short --release)
    printf "Ubuntu $lsb_rel "
    case $lsb_rel in
      ( 12.04 ) #
        echo "is supported."
        ;;
      ( * )
        echo "is UNTESTED." 
        ;;
    esac

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
      cmake

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
      libsasl2-dev

    for lib in libjpeg.so libfreetype.so libz.so ; do
      if ! readlink -e /usr/lib/x86_64-linux-gnu/${lib} ; then
    	sudo ln -s /usr/lib/x86_64-linux-gnu/${lib} /usr/lib
      fi
    done 

    cd $NEON_ROOT_DIR/externalLibs
    # libunwind 
    printf "Checking libunwind: "
    if readlink -e /usr/local/lib/libunwind-x86_64.so ; then
      echo "libunwind installed"
    else
      tar -xzf libunwind-0.99-beta.tar.gz
      cd libunwind-0.99-beta
      ./configure CFLAGS=-U_FORTIFY_SOURCE LDFLAGS=-L`pwd`/src/.libs
      sudo make install --yes
      cd ..
    fi

    # GPerfTools
    printf "Checking gperftools: "
    if readlink -e /usr/local/lib/libtcmalloc_minimal.so ; then
       echo "gperftools installed"
    else
      tar -xzf gperftools-2.1.tar.gz
      cd gperftools-2.1
      ./configure
      sudo make install --yes
      cd ..
    fi

    # https://sites.google.com/a/neon-lab.com/engineering/system-setup/dependencies#TOC-Fast-Fourier-Transform-Package-FFTW3-
    sudo apt-get install --yes fftw3-dev

    # GFlags
    ./install_gflags.sh

    # MySQL - https://sites.google.com/a/neon-lab.com/engineering/system-setup/dependencies#TOC-MySql
    sudo apt-get install --yes mysql-client

    # Redis - https://sites.google.com/a/neon-lab.com/engineering/system-setup/dependencies#TOC-Redis
    sudo apt-get install --yes redis-server

    # Python - 
    $NEON_ROOT_DIR/install_python_deps.sh

    # PCRE Perl lib (required for http rewrite module of nginx)
    sudo apt-get install --yes libpcre3 libpcre3-dev

    # Ruby
    ./install_ruby.sh

    # Hadoop
    ./install_hadoop.sh

    # OpenCV
    ./install_opencv.sh
    ;;
esac

# vim: set ts=2 sw=2 sts=2 expandtab #
