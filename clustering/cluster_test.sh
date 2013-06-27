#!/bin/bash

#Testset
dir=$1
clusters=$2

#Run clustering
python kmeans_clustering.py $dir $clusters

#Create Model
python create_model.py $dir"_"$clusters 1

#Calculate Precision for the model
modelDir="model_"$dir"_"$clusters
precision=$(python calc_model_precision.py $modelDir $dir 1 | awk '{print $2}')

echo $clusters $precision >>result 
