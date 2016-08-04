%MONGO_HOME%\bin\mongo < _03-import-document.js
pause
curl -XPOST "localhost:9200/mygeoindex/_search?pretty=true" -d @_03-geo-distance-query.json
pause
