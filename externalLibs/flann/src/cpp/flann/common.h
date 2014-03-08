/***********************************************************************
 * Software License Agreement (BSD License)
 *
 * Copyright 2008-2009  Marius Muja (mariusm@cs.ubc.ca). All rights reserved.
 * Copyright 2008-2009  David G. Lowe (lowe@cs.ubc.ca). All rights reserved.
 *
 * THE BSD LICENSE
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions
 * are met:
 *
 * 1. Redistributions of source code must retain the above copyright
 *    notice, this list of conditions and the following disclaimer.
 * 2. Redistributions in binary form must reproduce the above copyright
 *    notice, this list of conditions and the following disclaimer in the
 *    documentation and/or other materials provided with the distribution.
 *
 * THIS SOFTWARE IS PROVIDED BY THE AUTHOR ``AS IS'' AND ANY EXPRESS OR
 * IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES
 * OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED.
 * IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR ANY DIRECT, INDIRECT,
 * INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT
 * NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
 * DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
 * THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF
 * THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 *************************************************************************/
#ifndef FLANN_COMMON_H_
#define FLANN_COMMON_H_

#include "defines.h"
#include "flann/util/logger.h"
#include "flann/util/params.h"

#ifdef __cplusplus
extern "C"
{
    using namespace flann;
#endif

struct FLANNParameters
{
    enum flann_algorithm_t algorithm; /* the algorithm to use */

    /* search time parameters */
    int checks;                /* how many leafs (features) to check in one search */
    float eps;     /* eps parameter for eps-knn search */
    int sorted;     /* indicates if results returned by radius search should be sorted or not */
    int max_neighbors;  /* limits the maximum number of neighbors should be returned by radius search */
    int cores;      /* number of paralel cores to use for searching */

    /*  kdtree index parameters */
    int trees;                 /* number of randomized trees to use (for kdtree) */
    int leaf_max_size;

    /* kmeans index parameters */
    int branching;             /* branching factor (for kmeans tree) */
    int iterations;            /* max iterations to perform in one kmeans cluetering (kmeans tree) */
    enum flann_centers_init_t centers_init;  /* algorithm used for picking the initial cluster centers for kmeans tree */
    float cb_index;            /* cluster boundary index. Used when searching the kmeans tree */

    /* autotuned index parameters */
    float target_precision;    /* precision desired (used for autotuning, -1 otherwise) */
    float build_weight;        /* build tree time weighting factor */
    float memory_weight;       /* index memory weigthing factor */
    float sample_fraction;     /* what fraction of the dataset to use for autotuning */

    /* LSH parameters */
    unsigned int table_number_; /** The number of hash tables to use */
    unsigned int key_size_;     /** The length of the key in the hash tables */
    unsigned int multi_probe_level_; /** Number of levels to use in multi-probe LSH, 0 for standard LSH */

    /* other parameters */
    enum flann_log_level_t log_level;    /* determines the verbosity of each flann function */
    long random_seed;            /* random seed to use */
};


typedef void* FLANN_INDEX; /* deprecated */
typedef void* flann_index_t;

FLANN_EXPORT extern struct FLANNParameters DEFAULT_FLANN_PARAMETERS;

extern flann_distance_t flann_distance_type;
extern int flann_distance_order;

/**
   Sets the log level used for all flann functions (unless
   specified in FLANNParameters for each call

   Params:
    level = verbosity level
 */
FLANN_EXPORT void flann_log_verbosity(int level);


/**
 * Sets the distance type to use throughout FLANN.
 * If distance type specified is MINKOWSKI, the second argument
 * specifies which order the minkowski distance should have.
 */
FLANN_EXPORT void flann_set_distance_type(enum flann_distance_t distance_type, int order);

FLANN_EXPORT void init_flann_parameters(FLANNParameters* p);

#ifdef __cplusplus
}

#include "flann.hpp"

FLANN_EXPORT flann::IndexParams create_parameters(FLANNParameters* p);
FLANN_EXPORT flann::SearchParams create_search_params(FLANNParameters* p);

#endif

#endif // FLANN_COMMON_H_
