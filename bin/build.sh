#!/bin/bash
source ${BASH_SOURCE%/*}/clean.sh
poetry run pdoc csv_batcher -o ./docs --logo https://github.com/tangledpath/csv-batcher/blob/master/csv_batcher.png --favicon https://github.com/tangledpath/csv-batcher/blob/master/csv_batcher_sm.png
cp csv_batcher.png docs
poetry build
