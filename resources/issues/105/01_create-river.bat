curl -XPUT "http://localhost:9200/mydb105" -d @disabled-mapping.json
curl -XPUT "http://localhost:9200/mydb105/document/_mapping" -d @custom-mapping.json
pause
curl -XPUT "http://localhost:9200/_river/river105/_meta" -d @mongodb-river-simple.json