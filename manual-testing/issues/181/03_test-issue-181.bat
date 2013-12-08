%MONGO_HOME%\bin\mongo < _03-import-document.js
pause
curl -XPOST "localhost:9200/authors/author/_search?pretty=true" -d @_03-find-book-parent-query.json
pause
curl -XPOST "localhost:9200/authors/book/_search?pretty=true" -d @_03-find-chapter-parent-query.json
pause
