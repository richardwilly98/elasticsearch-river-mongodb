%MONGO_HOME%\bin\mongo < test-issue-105.js
pause
curl -XGET localhost:9200/mydb105/_search?pretty=true&q=firstName:John
pause
%MONGO_HOME%\bin\mongo < drop-collection-105.js
pause
curl -XGET localhost:9200/mydb105/_mapping?pretty=true