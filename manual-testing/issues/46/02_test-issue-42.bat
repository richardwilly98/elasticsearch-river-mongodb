%MONGO_HOME%\bin\mongo < test-issue-42-import-document.js
pause
curl -XGET localhost:9200/mydb-42/_search?q=firstName:John42
pause
%MONGO_HOME%\bin\mongo < test-issue-42-update-document.js
pause
curl -XGET localhost:9200/mydb-42/_search?q=firstName:John42
pause