%MONGO_HOME%\bin\mongo < test-import-document.js
pause
curl -XGET localhost:9200/mydb-89/_search?q=firstName:John
pause
