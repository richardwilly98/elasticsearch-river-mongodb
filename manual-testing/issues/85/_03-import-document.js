use mydb85

var author =
{
    "name": "Herge",
    "nationality": "Belge"
}

db.authors.save(author)

var author1 = db.authors.findOne()

var book = {
"_parentId": author1._id,
"name": "Titin au Congo",
"genre": "Bande dessinee",
"publisher": "Herge"
}

db.books.save(book)