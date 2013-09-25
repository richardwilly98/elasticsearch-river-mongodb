curl -XPUT "http://localhost:9200/_river/authors54/_meta" -d @_02_mongodb-river-author.json
PAUSE
curl -XPUT "http://localhost:9200/_river/books54/_meta" -d @_02_mongodb-river-book.json