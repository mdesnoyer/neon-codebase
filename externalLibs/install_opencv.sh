#!/bin/bash
# -------------------------------------------------
# build OpenCV
# -------------------------------------------------
# https://sites.google.com/a/neon-lab.com/engineering/system-setup/dependencies#TOC-OpenCV
set -e

# default: install CUDA libraries (run 'with_cuda=false ./install_opencv.sh')
if ${with_cuda:-true} ; then
  # If CUDA is desired:
  sudo apt-get install libxi-dev libxmu-dev freeglut3-dev build-essential binutils-gold
  sudo ln -s /usr/lib/x86_64-linux-gnu/libglut.so /usr/lib/
  wget http://developer.download.nvidia.com/compute/cuda/5_0/rel-update-1/installers/cuda_5.0.35_linux_64_ubuntu11.10-1.run .
  chmod +x cuda_5.0.35_linux_64_ubuntu11.10-1.run
  sudo ./cuda_5.0.35_linux_64_ubuntu11.10-1.run
  echo 'export PATH=$PATH:/usr/local/cuda-5.0/bin' >> ~/.bash_profile
  echo 'export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:/usr/local/cuda-5.0/lib64:/lib' >> ~/.bash_profile 
  source ~/.bash_profile
fi

# Install CMake
# Install QT (For better UI components)
# Install I/O libraries:
sudo apt-get install \
 cmake \
 libqt4-dev \
 libtiff4-dev libjasper-dev libavformat-dev libswscale-dev libavcodec-dev libjpeg-dev libpng-dev libv4l-dev

# Finally, install OpenCV. From the externalLibs directory:
# * NOTE: This must be performed AFTER you have acquired the repositories, and in the ./neon directory. 
release_dir=opencv-2.4.6/release
if [ -d $release_dir ] ; then 
  sudo rm -rf $release_dir
fi
tar -xzf opencv-2.4.6.tar.gz
mkdir $release_dir
cd $release_dir
cmake -D CMAKE_BUILD_TYPE=RELEASE -D WITH_QT=ON -D BUILD_PYTHON_SUPPORT=ON -D CMAKE_INSTALL_PREFIX=${PREFIX:-/usr/local} -D WITH_CUDA=ON -D WITH_CUBLAS=ON  ..
sudo make install

# vim: set ts=2 sts=2 sw=2 expandtab #
