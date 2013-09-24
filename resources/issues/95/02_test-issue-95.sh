#!/bin/bash

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

mongo mydb95 < "${DIR}/test-issue-95.js"
sleep 1
curl -XGET localhost:9200/mydb95/_search?q=content:test91
