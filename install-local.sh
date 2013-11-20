#!/bin/bash

# Installs the current version from this directory to your local elasticsearch installation for development.

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
PWD="`pwd`"

mvn -Dmaven.test.skip=true package
sudo /usr/share/elasticsearch/bin/plugin --remove elasticsearch-river-mongodb

VERSION=$(grep -E -m 1 -o "<version>(.*)</version>" pom.xml | sed -e 's,.*<version>\([^<]*\)</version>.*,\1,g' )

sudo /usr/share/elasticsearch/bin/plugin --url "file://${DIR}/target/releases/elasticsearch-river-mongodb-${VERSION}.zip" --install elasticsearch-river-mongodb

