#!/bin/bash

# Install CUDA
sudo apt-get install libxi-dev libxmu-dev freeglut3-dev build-essential binutils-gold
sudo ln -s /usr/lib/x86_64-linux-gnu/libglut.so /usr/lib/
wget http://developer.download.nvidia.com/compute/cuda/5_0/rel-update-1/installers/cuda_5.0.35_linux_64_ubuntu11.10-1.run .
chmod +x cuda_5.0.35_linux_64_ubuntu11.10-1.run
sudo ./cuda_5.0.35_linux_64_ubuntu11.10-1.run
echo 'export PATH=$PATH:/usr/local/cuda-5.0/bin' >> ~/.bash_profile
echo 'export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:/usr/local/cuda-5.0/lib64:/lib' >> ~/.bash_profile

# Install cmake
sudo apt-get install cmake

# Install python dev
sudo apt-get install python-dev

# Install QT
sudo apt-get install libqt4-dev

# Install image format processing libraries
sudo apt-get install libtiff4-dev libjasper-dev libavformat-dev libswscale-dev libavcodec-dev libjpeg-dev libpng-dev libv4l-dev

# Finally, install opencv
tar -xzf opencv-2.4.6.tar.gz
mkdir opencv-2.4.6/release
cd opencv-2.4.6/release
cmake -D CMAKE_BUILD_TYPE=RELEASE -D WITH_QT=ON -D BUILD_PYTHON_SUPPORT=ON -D CMAKE_INSTALL_PREFIX=/usr/local -D WITH_CUDA=ON -D WITH_CUBLAS=ON  ..
sudo make install