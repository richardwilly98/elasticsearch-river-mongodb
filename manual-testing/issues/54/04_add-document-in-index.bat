curl -XPUT "http://localhost:9200/mygeoindex/pin/1" -d @_04-add-paris-document-in-index.json
curl -XPUT "http://localhost:9200/mygeoindex/pin/2" -d @_04-add-nyc-document-in-index.json
curl -XPUT "http://localhost:9200/mygeoindex/pin/3" -d @_04-add-london-document-in-index.json