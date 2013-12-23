%MONGO_HOME%\bin\mongo < 02-map-reduce.js
pause
rem curl -XPOST "localhost:9200/authors/author/_search?pretty=true" -d @_03-find-book-parent-query.json
pause
rem curl -XPOST "localhost:9200/authors/book/_search?pretty=true" -d @_03-find-chapter-parent-query.json
pause
