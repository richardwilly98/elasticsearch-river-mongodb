%MONGO_HOME%\bin\mongo < test-issue-26.js
pause
curl -XGET localhost:9200/mydb/_search?q=enrollment:390