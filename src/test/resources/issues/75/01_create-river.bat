curl -XPUT "http://localhost:9200/index75" -d @disabled-mapping.json
curl -XPUT "http://localhost:9200/index75/document/_mapping" -d @custom-mapping.json
curl -XPUT "http://localhost:9200/_river/river75/_meta" -d @mongodb-river-simple.json