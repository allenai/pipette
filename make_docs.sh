#!/usr/bin/env bash

set -euxo pipefail

# Use `./make_docs.sh full` when you recreate the docs from scratch.
# Use `./make_docs.sh` when you are refreshing the docs from the docstrings in the source.

if [ "${1:-}" = "full" ]; then
  sphinx-apidoc -f -M -A 'Dirk Groeneveld' -H 'Pipette' -a --full -o doc/ pipette/ pipette/asciidag
else
  sphinx-apidoc -f -M -A 'Dirk Groeneveld' -H 'Pipette' -a --tocfile index -o doc/ pipette/ pipette/asciidag
fi

pushd doc
make singlehtml
popd

rm -r docs
mkdir docs
mv doc/_build/singlehtml/* ./docs/
