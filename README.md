MongoDB River Plugin for ElasticSearch
==================================

    ---------------------------------------------------------
    | MongoDB River Plugin     | ElasticSearch    | MongoDB |
    --------------------------------------------------------|
    | master                   | 0.20.5 -> master | 2.2.3   |
    --------------------------------------------------------|
    | 1.6.2                    | 0.20.1           | 2.2.2   |
    --------------------------------------------------------|
    | 1.6.0                    | 0.20.1 -> master | 2.2.2   |
    --------------------------------------------------------|
    | 1.5.0                    | 0.19.11          | 2.2.1   |
    --------------------------------------------------------|
    | 1.4.0                    | 0.19.8           | 2.0.5   |
    --------------------------------------------------------|
    | 1.3.0                    | 0.19.4           |         |
    --------------------------------------------------------|
    | 1.2.0                    | 0.19.0           |         |
    --------------------------------------------------------|
    | 1.1.0                    | 0.19.0           | 2.0.2   |
    --------------------------------------------------------|
    | 1.0.0                    | 0.18             |         |
    --------------------------------------------------------|

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
			"collection": "fs", 
			"gridfs": true 
		}, 
		"index": {
			"name": "mongoindex", 
			"type": "files" 
		}
	}'

Import binary content in mongo:

	%MONGO_HOME%\bin>mongofiles.exe --host localhost:27017 --db testmongo --collection fs put test-document-2.pdf
	connected to: localhost:27017
	added file: { _id: ObjectId('4f230588a7da6e94984d88a1'), filename: "test-document-2.pdf", chunkSize: 262144, uploadDate: new Date(1327695240206), md5: "c2f251205576566826f86cd969158f24", length: 173293 }
	done!

Query index:

	curl -XGET 'http://localhost:9200/files/4f230588a7da6e94984d88a1?pretty=true'

See more details check the [wiki](https://github.com/richardwilly98/elasticsearch-river-mongodb/wiki)

License
-------

    This software is licensed under the Apache 2 license, quoted below.

    Copyright 2009-2012 Shay Banon and ElasticSearch <http://www.elasticsearch.org>

    Licensed under the Apache License, Version 2.0 (the "License"); you may not
    use this file except in compliance with the License. You may obtain a copy of
    the License at

        http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
    WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
    License for the specific language governing permissions and limitations under
    the License.

Changelog
-------

	#### 1.6.0 
	- Support for sharded collection
	- Script filters
	- MongoDB driver 2.10.1 (use of MongoClient)

	#### 1.6.2 
	- Support for secured sharded collection (see issue #60)

	#### 1.6.3 
	- First attempt to stored the artifact in Maven central (please ignore this version

	#### 1.6.2 
	- Fix NPE (see issue #60)
	- Remove database user, password river settings. Local or admin user, password should be used instead.
	