#!/bin/bash

set -ex

rsync -avz --progress --chown=nuks:def-porban --exclude-from=.gitignore --no-perms --no-group . nuks@beluga.calculcanada.ca:/project/def-porban/shared/preprocessing
