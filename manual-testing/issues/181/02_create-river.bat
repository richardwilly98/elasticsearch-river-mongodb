curl -XPUT "http://localhost:9200/_river/authors181/_meta" -d @_02_mongodb-river-author.json
PAUSE
curl -XPUT "http://localhost:9200/_river/books181/_meta" -d @_02_mongodb-river-book.json
PAUSE
curl -XPUT "http://localhost:9200/_river/chapter181/_meta" -d @_02_mongodb-river-chapter.json
