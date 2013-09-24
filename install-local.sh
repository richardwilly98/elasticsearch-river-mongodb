#!/bin/bash

# Installs the current version from this directory to your local elasticsearch installation for development.

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
PWD="`pwd`"

mvn -Dmaven.test.skip=true package
sudo /usr/share/elasticsearch/bin/plugin --remove elasticsearch-river-mongodb
sudo /usr/share/elasticsearch/bin/plugin --url "file://${DIR}/target/releases/elasticsearch-river-mongodb-1.7.1-SNAPSHOT.zip" --install elasticsearch-river-mongodb
