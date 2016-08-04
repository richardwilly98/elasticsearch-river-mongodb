curl -XPUT "http://localhost:9200/_river/authors85/_meta" -d @_02_mongodb-river-author.json
PAUSE
curl -XPUT "http://localhost:9200/_river/books85/_meta" -d @_02_mongodb-river-book.json