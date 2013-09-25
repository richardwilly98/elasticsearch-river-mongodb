use mydb
var o = db.mycollec.findOne({"firstName": "John42", "lastName": "Doe42"})
o.state = 'CLOSED';
db.mycollec.save(o)