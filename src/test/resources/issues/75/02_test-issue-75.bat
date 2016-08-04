%MONGO_HOME%\bin\mongo < test-issue-75.js
pause
curl -XGET localhost:9200/index75/_search?pretty=true&q=firstName:John