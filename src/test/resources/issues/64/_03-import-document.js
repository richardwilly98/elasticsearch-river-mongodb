use mydb
var author =
{
	"_id": "herge",
	"name": "Herge",
	"nationality": "Belge"
}

db.authors.save(author)

var book = {
	"_parentId": "herge",
	"name": "Titin au Congo",
	"genre": "Bande dessinee",
	"publisher": "Herge"
}

db.books.save(book)
