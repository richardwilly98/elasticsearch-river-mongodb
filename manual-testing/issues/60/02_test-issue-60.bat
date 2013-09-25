%MONGO_HOME%\bin\mongo < test-issue-60-import-document.js
pause
curl -XGET localhost:9200/mydb60/_search?q=firstName:John60
pause
%MONGO_HOME%\bin\mongo < test-issue-60-update-document.js
pause
curl -XGET localhost:9200/mydb60/_search?q=firstName:John60
pause