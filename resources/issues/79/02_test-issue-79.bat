%MONGO_HOME%\bin\mongo < test-issue-79-1.js
pause
%MONGO_HOME%\bin\mongo < test-issue-79-2.js
pause
curl -XGET localhost:9200/mydb79/_search?q=name:richard