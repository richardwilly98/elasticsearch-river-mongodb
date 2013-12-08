var status = rs.status()
 for (i=0; i < status.members.length; i++) {
   if (status.members[i].state == 1) {
     db = connect(status.members[i].name + "/local")
   }
 }

use mydb181
var author =
{
	"_id": "herge",
	"name": "Herge",
	"nationality": "Belge"
}

db.authors.save(author)

var book = {
  "_id": "tintin-au-congo",
	"_parentId": "herge",
	"name": "Tintin au Congo",
	"genre": "Bande dessinee",
	"publisher": "Herge"
}

db.books.save(book)

var chapter = {
  "_parentId": "tintin-au-congo",
  "title": "Introduction",
  "page": 1
}

db.chapters.save(chapter)
