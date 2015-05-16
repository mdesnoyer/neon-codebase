#!/bin/bash
cat <<EOF
# -------------------------------------------------
# Build OpenCV
# -------------------------------------------------
EOF
# https://sites.google.com/a/neon-lab.com/engineering/system-setup/dependencies#TOC-OpenCV

PS4="+OPENCV: "
set -ex

# default: install CUDA libraries (run 'with_cuda=false ./install_opencv.sh')
if ${with_cuda:-true} ; then
  # If CUDA is desired:
  sudo apt-get install --yes libxi-dev libxmu-dev freeglut3-dev build-essential binutils-gold
  if ! readlink -e /usr/lib/x86_64-linux-gnu/libglut.so ; then
    sudo ln -s /usr/lib/x86_64-linux-gnu/libglut.so /usr/lib/
  fi
  cuda=cuda_5.0.35_linux_64_ubuntu11.10-1.run
  cuda_path=/usr/local/cuda-5.0
  if ! [ -d ${cuda_path}/lib64 ] ; then
    if ${cuda_from_s3:-true} ; then
      sudo pip install awscli
      aws s3 cp s3://neon-apt-us-east-1/${cuda} .
    else
      wget http://developer.download.nvidia.com/compute/cuda/5_0/rel-update-1/installers/${cuda} .
    fi
    chmod +x ./${cuda}
    sudo ./${cuda} -silent -toolkit -toolkitpath=${cuda_path}
    echo "export PATH=\$PATH:${cuda_path}/bin" >> ~/.bash_profile
    echo "export LD_LIBRARY_PATH=\$LD_LIBRARY_PATH:${cuda_path}/lib64:/lib" >> ~/.bash_profile 
  fi
  source ~/.bash_profile
fi

# Install CMake
# Install QT (For better UI components)
# Install I/O libraries:
sudo apt-get install --yes \
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
