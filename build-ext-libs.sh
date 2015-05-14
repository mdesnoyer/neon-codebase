
#!/bin/bash
# -----------------------------------------------------------
# Build the local Neon External Libraries: PProf & libunwind
# -----------------------------------------------------------
# https://sites.google.com/a/neon-lab.com/engineering/system-setup/dependencies
#
set -e
dir=$(dirname $0)

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
    cd $dir/externalLibs

    # LibUnWind
    printf "Checking libunwind: "
    if readlink -e /usr/local/lib/libunwind-x86_64.so ; then
      echo "libunwind installed"
    else
      tar -xzf libunwind-0.99-beta.tar.gz
      cd libunwind-0.99-beta
      ./configure CFLAGS=-U_FORTIFY_SOURCE LDFLAGS=-L`pwd`/src/.libs
      sudo make install
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
      sudo make install
      cd ..
    fi
    ;;
esac

# vim: set ts=2 sw=2 sts=2 expandtab #
