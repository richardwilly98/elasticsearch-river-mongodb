%MONGO_HOME%\bin\mongo < test-issue-175.js
pause
curl -XGET localhost:9200/maindb/venue/_search?q=title:Developer
pause