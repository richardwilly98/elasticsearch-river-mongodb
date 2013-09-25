%MONGO_HOME%\bin\mongo < test-issue-101.js
pause
curl -XGET localhost:9200/mydb101/_search?q=collection:mycollec101