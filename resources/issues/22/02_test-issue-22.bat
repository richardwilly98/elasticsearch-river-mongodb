%MONGO_HOME%\bin\mongo < test-issue-22.js
pause
curl -XGET localhost:9200/mydb/_search?q=title:Developer
pause