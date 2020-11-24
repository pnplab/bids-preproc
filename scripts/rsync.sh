#!/bin/bash

set -ex

echo "pwd: $(pwd)"
# rsync -avz --progress --exclude-from=.gitignore --no-perms --no-group . nuks@192.168.0.18:~/Documents/preprocessing
rsync -avz --progress --chown=nuks:def-porban --exclude-from=.gitignore --no-perms --no-group . nuks@beluga.calculcanada.ca:/project/def-porban/shared/preprocessing
rsync -avz --progress --exclude-from=.gitignore --no-perms --no-group . tpiront@elm.criugm.qc.ca:~/preprocessing
