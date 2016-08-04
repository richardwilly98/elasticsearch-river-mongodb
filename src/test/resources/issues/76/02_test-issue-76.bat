%MONGO_HOME%\bin\mongo < test-issue-76.js
pause
curl -XGET localhost:9200/mydb76/_search?q=enrollment:390