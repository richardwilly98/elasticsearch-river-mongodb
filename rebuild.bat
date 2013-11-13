SET ES_HOME=C:\Dev\elasticsearch-0.90.5
SET RIVER_VERSION=1.7.2-SNAPSHOT

CALL mvn clean package -Dmaven.test.skip=true 
CALL %ES_HOME%\bin\plugin -r elasticsearch-river-mongodb
CALL %ES_HOME%\bin\plugin -i elasticsearch-river-mongodb -u file:///%CD%/target/releases/elasticsearch-river-mongodb-%RIVER_VERSION%.zip
