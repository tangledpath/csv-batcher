#!/bin/bash
source ${BASH_SOURCE%/*}/clean.sh
# Run in browser:
poetry run pdoc csv_batcher --logo https://raw.githubusercontent.com/tangledpath/csv-batcher/master/csv_batcher.png --favicon https://raw.githubusercontent.com/tangledpath/csv-batcher/master/csv_batcher_sm.png
poetry run pdoc csv_batcher -o ./docs --logo https://raw.githubusercontent.com/tangledpath/csv-batcher/master/csv_batcher.png --favicon https://raw.githubusercontent.com/tangledpath/csv-batcher/master/csv_batcher_sm.png
poetry build
