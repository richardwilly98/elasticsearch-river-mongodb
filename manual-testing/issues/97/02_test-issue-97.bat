%MONGO_HOME%\bin\mongo < test-import-document.js
pause
curl -XGET localhost:9200/mydb-97/_search?pretty=true&q=scores:89
pause
