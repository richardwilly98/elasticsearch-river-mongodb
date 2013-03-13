%MONGO_HOME%\bin\mongo admin -u admin -p admin < test-issue-61.js
pause
curl -XGET localhost:9200/mydb/_search?q=firstName:John-61
