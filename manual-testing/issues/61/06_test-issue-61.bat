%MONGO_HOME%\bin\mongo --port 27018 local -u local -p local < test-issue-61.js
pause
curl -XGET localhost:9200/mydb61/_search?q=firstName:John-61
