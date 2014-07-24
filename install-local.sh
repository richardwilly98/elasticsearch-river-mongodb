#!/bin/bash

# Installs the current version from this directory to your local elasticsearch installation for development.

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
PWD="`pwd`"
ES_HOME=${ES_HOME:-/usr/share/elasticsearch}

mvn -Dmaven.test.skip=true package
sudo $ES_HOME/bin/plugin --remove elasticsearch-river-mongodb

VERSION=$(grep -E -m 1 -o "<version>(.*)</version>" pom.xml | sed -e 's,.*<version>\([^<]*\)</version>.*,\1,g' )

sudo $ES_HOME/bin/plugin --url "file://${DIR}/target/releases/elasticsearch-river-mongodb-${VERSION}.zip" --install elasticsearch-river-mongodb


