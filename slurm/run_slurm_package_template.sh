#!/usr/bin/env bash

set -Exeuo pipefail

PROJECT_NAME=$1
shift

mkdir -p ~/slurm_workdirs/
WORKDIR=$(mktemp -d -p ~/slurm_workdirs/ "$PROJECT_NAME.XXXX")
mkdir -p "$WORKDIR"

ARCHIVE=$(awk '/^__ARCHIVE_BELOW__/ {print NR + 1; exit 0; }' $0)
tail -n+$ARCHIVE $0 | perl -pe 'chomp if eof' | tar -xzv -C "$WORKDIR"
cd "$WORKDIR"

# Install conda if it isn't there.
if [ -d $HOME/miniconda ]; then
  CONDA=$HOME/miniconda/condabin/conda
elif [ -d $HOME/miniconda3 ]; then
  CONDA=$HOME/miniconda3/condabin/conda
else
  wget "https://repo.anaconda.com/miniconda/Miniconda3-latest-Linux-x86_64.sh" -O miniconda.sh
  bash miniconda.sh -b -p $HOME/miniconda
  rm miniconda.sh
  CONDA=$HOME/miniconda/condabin/conda
  $CONDA init bash
fi
$CONDA update -q -y conda

# install conda dependencies
CONDA_ENV=$(basename $WORKDIR)
if [ -f conda-requirements.txt ]; then
  $CONDA create -q -y -n $CONDA_ENV
  cat conda-requirements.txt | while read p; do
    $CONDA install -y -q -n $CONDA_ENV $p
  done
else
  $CONDA create -q -y -n $CONDA_ENV python=3.6
fi

# install pip dependencies
if [ -f requirements.txt ]; then
  $CONDA run -n $CONDA_ENV pip install -r requirements.txt
fi

# run the script
set +e
$CONDA run -n $CONDA_ENV env PYTHONPATH=. "$@"
R=$?
$CONDA remove -q -y --name $CONDA_ENV --all
cd ~/slurm_workdirs
rm -r "$WORKDIR"

exit $R

__ARCHIVE_BELOW__
