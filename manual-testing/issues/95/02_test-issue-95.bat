%MONGO_HOME%\bin\mongo mydb95 < test-issue-95.js
pause
curl -XGET localhost:9200/mydb95/_search?q=content:test91