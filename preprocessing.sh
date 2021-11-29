#!/usr/bin/env bash

module load python/3.9.6
python -m venv venv
source venv/bin/activate
python -m pip install -r requirements.txt

python main.py "$@"

deactivate
