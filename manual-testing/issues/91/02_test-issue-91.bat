%MONGO_HOME%\bin\mongofiles --host localhost:27018 --db mydb91 --collection mycollec91 --type applicaton/pdf put test-document.pdf
%MONGO_HOME%\bin\mongo < test-issue-91.js
pause
curl -XGET localhost:9200/mydb91/_search?q=metadata.titleDoc:test91