%MONGO_HOME%\bin\mongo < test-import-document.js
pause
curl -XGET "localhost:9200/media/_search?pretty=true&q=user:joe.doe"
pause
%MONGO_HOME%\bin\mongo < test-update-document.js
pause
curl -XGET "localhost:9200/media/_search?pretty=true&q=user:joe.doe"
pause
