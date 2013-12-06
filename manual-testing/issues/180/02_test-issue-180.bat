%MONGO_HOME%\bin\mongo < test-import-document-01.js
pause
curl -XGET localhost:9200/mydb180/_search?q=firstName:John
pause
%MONGO_HOME%\bin\mongo < test-import-document-02.js
pause
curl -XGET localhost:9200/mydb180/_search?q=firstName:John
pause
