#!/bin/bash

#Testset
dir=$1
clusters=$2
valence_scores=$3

#Run clustering
python kmeans_clustering.py --in_dir $dir -n $clusters -i $valence_scores

#Create Model
python create_model.py $dir"_"$clusters 1

#Calculate Precision for the model
modelDir=$dir"_"$clusters"_model"
precision=$(python calc_model_precision.py $modelDir $dir 1 | awk '{print $2}')

echo $clusters $precision >>result 
