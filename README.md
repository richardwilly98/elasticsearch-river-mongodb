MongoDB River Plugin for ElasticSearch
==================================

| MongoDB River Plugin     | ElasticSearch    | MongoDB |
|--------------------------|------------------|---------|
| master                   | 0.90.2 -> master | 2.4.5   |
| 1.6.11                   | 0.90.2           | 2.4.5   |
| 1.6.9                    | 0.90.1           | 2.4.4   |
| 1.6.8                    | 0.90.0           | 2.4.3   |
| 1.6.7                    | 0.90.0           | 2.4.3   |
| 1.6.6                    | 0.90.0           | 2.4.3   |
| 1.6.5                    | 0.20.6           | 2.4.1   |
| 1.6.4                    | 0.20.5           | 2.2.3   |
| 1.6.2                    | 0.20.1           | 2.2.2   |
| 1.6.0                    | 0.20.1 -> master | 2.2.2   |
| 1.5.0                    | 0.19.11          | 2.2.1   |
| 1.4.0                    | 0.19.8           | 2.0.5   |
| 1.3.0                    | 0.19.4           |         |
| 1.2.0                    | 0.19.0           |         |
| 1.1.0                    | 0.19.0           | 2.0.2   |
| 1.0.0                    | 0.18             |         |

Build status
-------

[![Build Status](https://buildhive.cloudbees.com/job/richardwilly98/job/elasticsearch-river-mongodb/badge/icon)](https://buildhive.cloudbees.com/job/richardwilly98/job/elasticsearch-river-mongodb/)

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

#### 1.6.11
- Add SSL support by @alistair (see [#94](https://github.com/richardwilly98/elasticsearch-river-mongodb/pull/94))
- Add support for $set operation (see issue [#91](https://github.com/richardwilly98/elasticsearch-river-mongodb/issues/91))
- Add Groovy unit test (for feature [#87](https://github.com/richardwilly98/elasticsearch-river-mongodb/issues/87))
- Update versions ES 0.90.2, MongoDB 2.4.5 and MongoDB driver 2.11.2
- Fix for ```options/drop_collection``` option (issue [#79](https://github.com/richardwilly98/elasticsearch-river-mongodb/issues/79))
- New ```options/include_collection``` parameter to include the collection name in the document indexed. (see [#101](https://github.com/richardwilly98/elasticsearch-river-mongodb/issues/101))

#### 1.6.9
- Allow the script filters to modify the document id (see [#83](https://github.com/richardwilly98/elasticsearch-river-mongodb/pull/83))
- Support for Elasticsearch 0.90.1 and MongoDB 2.4.4
- Improve exclude fields (support multi-level - see [#76](https://github.com/richardwilly98/elasticsearch-river-mongodb/issues/76))
- Fix to support ObjectId (see issue [#85](https://github.com/richardwilly98/elasticsearch-river-mongodb/issues/85))
- Add logger object to script filters
- Provide example for Groovy script (see issue[#87](https://github.com/richardwilly98/elasticsearch-river-mongodb/issues/87))

#### 1.6.8 
- Implement exclude fields (see issue #76)
- Improve reconnection to MongoDB when connection is lost (see issue #77)
- Implement drop collection feature (see issue #79). The river will drop all documents from the index type.

#### 1.6.7 
- Issue with sharded collection (see issue #46)

#### 1.6.6 
- Support for Elasticsearch 0.90.0 and MongoDB 2.4.3
- MongoDB driver 2.11.1 (use of MongoClient)	

#### 1.6.5 
- Add support for _parent, _routing (see issue #64)

#### 1.6.4 
- Fix NPE (see issue #60)
- Remove database user, password river settings. Local or admin user, password should be used instead.

#### 1.6.3 
- First attempt to stored the artifact in Maven central (please ignore this version

#### 1.6.2 
- Support for secured sharded collection (see issue #60)

#### 1.6.0 
- Support for sharded collection
- Script filters
- MongoDB driver 2.10.1 (use of MongoClient)

