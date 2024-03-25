#!/bin/bash
source ${BASH_SOURCE%/*}/clean.sh
poetry run pdoc csv_batcher -o ./docs
cp csv_batcher.png docs
poetry build
