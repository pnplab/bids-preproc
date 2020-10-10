#!/bin/bash
# cf. https://fmriprep.org/en/stable/singularity.html#templateflow-and-singularity

TMP_FOLDER='/tmp/dltplflow'
OUT_PATH='./templateflow'

python3 -m venv ${TMP_FOLDER}/templateflow_python_venv
source ${TMP_FOLDER}/templateflow_python_venv/bin/activate

# Install templateflow
python3 -m pip install -U templateflow

# Download templateflow dataset
echo "
from templateflow import api
api.TF_S3_ROOT = 'http://templateflow.s3.amazonaws.com'
api.get('MNI152NLin6Asym')
api.get('MNI152NLin2009cAsym')
api.get('OASIS30ANTs')
api.get('fsaverage') # for freesurfer reconstruction ?
api.get('fsLR') # remove unused bids filter ??
" | TEMPLATEFLOW_HOME=$OUT_PATH python3

deactivate

rm -r ${TMP_FOLDER}
