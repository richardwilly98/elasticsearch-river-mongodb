curl -XPUT "http://localhost:9200/_river/mongodb/_meta" -d "{ \"type\": \"mongodb\", \"mongodb\":{ \"db\": \"DATABASE_NAME\", \"collection\": \"COLLECTION\", \"index\": \"ES_INDEX_NAME\" }}"

Example:
curl -XPUT "http://localhost:9200/_river/mongodb/_meta" -d "{ \"type\": \"mongodb\", \"mongodb\":{ \"db\": \"testsbp\", \"collection\": \"person\", \"index\": \"personindex\" }}"

Query index:
curl -XGET "http://localhost:9200/testsbp/_search?q=firstName:Richard"