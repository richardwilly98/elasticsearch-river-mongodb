REM Copy joda-time-x-y.jar in $ES_HOME\lib
curl -XPUT "http://localhost:9200/_river/mongodb87/_meta" -d @mongodb-river-groovy-jodatime.json