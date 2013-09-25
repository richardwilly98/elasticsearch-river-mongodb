#!/bin/bash

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

curl -XPUT "http://localhost:9200/_river/river95/_meta" -d @"${DIR}/mongodb-simple-river.json"
