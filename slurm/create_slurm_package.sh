#!/usr/bin/env bash

set -Exeuo pipefail

CONTEXT_DIR=$1
OUTPUT_FILE=$2

cp run_slurm_package_template.sh $OUTPUT_FILE
tar -czOC "$CONTEXT_DIR" . --exclude-backups --exclude-vcs --exclude-vcs-ignores --exclude="*.pyc" --exclude="__pycache__" --exclude="pytorch-pretrained-bert-cache" --exclude="target" >> $OUTPUT_FILE
chmod +x $OUTPUT_FILE
