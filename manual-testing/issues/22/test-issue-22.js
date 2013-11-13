var status = rs.status()
 for (i=0; i < status.members.length; i++) {
   if (status.members[i].state == 1) {
     db = connect(status.members[i].name + "/local")
   }
 }

use mydb
var o =
{
	"firstName": "John",
	"lastName": "Doe",
	"age": 34,
	"title_from_mongo": "Developer"
}

db.mycollec.save(o)