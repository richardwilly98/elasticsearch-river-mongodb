%MONGO_HOME%\bin\mongo < _03-import-document.js
pause
curl -XPOST "localhost:9200/authors/author/_search?pretty=true" -d @_03-has-child-query.json
pause
