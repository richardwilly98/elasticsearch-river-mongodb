use mydb
var o =
{
	"firstName": "John",
	"lastName": "Doe",
	"age": 34,
	"title_from_mongo": "Developer"
}

db.mycollec.save(o)