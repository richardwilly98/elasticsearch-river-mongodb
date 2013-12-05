%MONGO_HOME%\bin\mongo < test-issue-177.js
pause
curl -XGET localhost:9200/mydb177/mycollect1/_count
pause
curl -XGET localhost:9200/mydb177/mycollect2/_count
pause