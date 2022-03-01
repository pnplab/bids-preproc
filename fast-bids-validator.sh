#!/usr/bin/env bash

set -ex

DATASET_PATH=$1
SIMG_PATH=${2:-"../singularity-images/bids-validator-1.8.5.simg"}

module load singularity
singularity run -B "${DATASET_PATH}:/dataset:ro" ${SIMG_PATH} /dataset
