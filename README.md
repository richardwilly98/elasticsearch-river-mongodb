MongoDB River Plugin for ElasticSearch

    -----------------------------------------------
    | MongoDB River Plugin     | ElasticSearch    |
    -----------------------------------------------
    | master                   | 0.19.8 -> master |
    -----------------------------------------------
    | 1.3.0                    | 0.19.4           |
    -----------------------------------------------
    | 1.2.0                    | 0.19.0           |
    -----------------------------------------------
    | 1.1.0                    | 0.19.0           |
    -----------------------------------------------
    | 1.0.0                    | 0.18             |
    -----------------------------------------------

Initial implementation by [aparo](https://github.com/aparo)

Modified to get the same structure as the others Elasticsearch river (like [CouchDB](http://www.elasticsearch.org/blog/2010/09/28/the_river_searchable_couchdb.html))

The latest version monitor oplog capped collection and support attachment (GridFS).

For the initial implementation see [tutorial](http://www.matt-reid.co.uk/blog_post.php?id=68#&slider1=4)


	curl -XPUT 'http://localhost:9200/_river/mongodb/_meta' -d '{
		"type": "mongodb", 
		"mongodb": { 
			"db": "DATABASE_NAME", 
			"collection": "COLLECTION", 
			"gridfs": true
		}, 
		"index": { 
			"name": "ES_INDEX_NAME", 
			"type": "ES_TYPE_NAME" 
		}
	}'

Example:

	curl -XPUT 'http://localhost:9200/_river/mongodb/_meta' -d '{ 
		"type": "mongodb", 
		"mongodb": { 
			"db": "testmongo", 
			"collection": "person"
		}, 
		"index": {
			"name": "mongoindex", 
			"type": "person" 
		}
	}'

Import data from mongo console:

	use testmongo
	var p = {firstName: "John", lastName: "Doe"}
	db.person.save(p)

Query index:

	curl -XGET 'http://localhost:9200/testmongo/_search?q=firstName:John'

	curl -XPUT 'http://localhost:9200/_river/mongodb/_meta' -d '{ 
		"type": "mongodb", 
		"mongodb": { 
			"db": "testmongo", 
			"collection": "files", 
			"gridfs": true 
		}, 
		"index": {
			"name": "mongoindex", 
			"type": "files" 
		}
	}'

Import binary content in mongo:

	%MONGO_HOME%\bin>mongofiles.exe --host localhost:27017 --db testmongo --collection files put test-document-2.pdf
	connected to: localhost:27017
	added file: { _id: ObjectId('4f230588a7da6e94984d88a1'), filename: "test-document-2.pdf", chunkSize: 262144, uploadDate: new Date(1327695240206), md5: "c2f251205576566826f86cd969158f24", length: 173293 }
	done!

Query index:

	curl -XGET 'http://localhost:9200/files/4f230588a7da6e94984d88a1?pretty=true'

See more details check the [wiki](https://github.com/richardwilly98/elasticsearch-river-mongodb/wiki)
