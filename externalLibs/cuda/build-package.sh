#!/bin/bash
# ---------------------------------------------------
# Build a debian package of Nvidia CUDA Toolkit
# ---------------------------------------------------
# The script will attempt to download the cuda .run file from Neon Labs' S3 bucket 
# unless executed with the environment variable `cuda_from_s3=false`, in which case
# it will download from Nvidia.
#
# Depends on the following software:
#   awscli - pip install awscli
#   fpm - gem install fpm
# 
# Debian packages: 
#   libxi-dev libxmu-dev freeglut3-dev build-essential binutils-gold
# 
# LDConfig
# The package installs binaries and header files into /usr/local/{bin,lib,lib64,include,etc...}
# and updates ldconfig accordingly. See the after-install.sh script.
#
cuda_version=5.0.35
cuda=cuda_${cuda_version}_linux_64_ubuntu11.10-1.run
cuda_path=$HOME/src/package-cuda
if ! [ -d ${cuda_path}/lib64 ] ; then
  if ! [ -f $cuda ] ; then
    if ${cuda_from_s3:-true} ; then
      aws s3 cp s3://neon-apt-us-east-1/${cuda} .
    else
      wget http://developer.download.nvidia.com/compute/cuda/5_0/rel-update-1/installers/${cuda} .
    fi
  fi
chmod +x ./${cuda}
sudo ./${cuda} -silent -toolkit -toolkitpath=${cuda_path}/usr/local
fi

# use FPM to build the .deb - https://github.com/jordansissel/fpm/wiki/PackageMakeInstall
fpm -s dir -t deb --name cuda --version ${cuda_version} --iteration 1 \
  -C ${cuda_path} \
  --force \
  --description "Nvidia CUDA Toolkit with header files (Neon Labs)" \
  --maintainer "build@neon-lab.com" \
  --url "https://developer.nvidia.com/cuda-zone" \
  --after-install after-install.sh \
  --after-remove after-remove.sh \
  -p $HOME/src/cuda-${cuda_version}_amd64.deb \
  --exclude doc \
  usr/local
