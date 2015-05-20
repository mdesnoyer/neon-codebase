#!/bin/bash
# ---------------------------------------------------
# Build a debian package of OpenCV
# ---------------------------------------------------
#
# Depends on the following software:
#   fpm - gem install fpm
# 
# Debian packages: 
#    cmake libqt4-dev libtiff4-dev libjasper-dev libavformat-dev libswscale-dev libavcodec-dev libjpeg-dev libpng-dev libv4l-dev 
#

CURDIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
cd ${CURDIR}

# build steps
opencv_version=2.4.6
opencv_path=$HOME/src/package-opencv
opencv_deb=$HOME/src/opencv-${opencv_version}_amd64.deb
mkdir -p ${opencv_path}

cd ..
release_dir=opencv-2.4.6/release
if [ -d $release_dir ] ; then
  sudo rm -rf $release_dir
fi
tar -xzf opencv-2.4.6.tar.gz
mkdir $release_dir
cd $release_dir
echo "Building OpenCV ${opencv_version}"
cmake -D CMAKE_BUILD_TYPE=RELEASE -D WITH_QT=ON -D BUILD_PYTHON_SUPPORT=ON -D CMAKE_INSTALL_PREFIX=/usr/local -D WITH_CUDA=ON -D WITH_CUBLAS=ON  ..
make
sudo make install DESTDIR=${opencv_path}

# dist-packages vs. site-packages in a virtualenv
# * http://stackoverflow.com/questions/9387928/whats-the-difference-between-dist-packages-and-site-packages
# * http://stackoverflow.com/questions/19210964/on-ubuntu-how-to-get-virtualenv-to-use-dist-packages
# we do this so that with this package installed, virtualenv's will be able to use OpenCV module 'cv'
cd ${opencv_path}/usr/local/lib/python2.7
mv dist-packages site-packages

cd $CURDIR
echo "Packaging OpenCV: ${opencv_deb}"
# use FPM to build the .deb - https://github.com/jordansissel/fpm/wiki/PackageMakeInstall
fpm -s dir -t deb --name opencv --version ${opencv_version} --iteration 2 \
  -C ${opencv_path} \
  --force \
  --description "OpenCV with header files and Python 2.7 module for virtualenvs (Neon Labs)" \
  --maintainer "build@neon-lab.com" \
  --url "http://opencv.org/" \
  --after-install after-install.sh \
  --after-remove after-remove.sh \
  --package ${opencv_deb} \
  --exclude doc \
  usr/local

# vim: set ts=2 sts=2 sw=2 expandtab #
