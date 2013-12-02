var status = rs.status()
 for (i=0; i < status.members.length; i++) {
   if (status.members[i].state == 1) {
     db = connect(status.members[i].name + "/local")
   }
 }

use mydb42
var o =
{
	"firstName": "John42",
	"lastName": "Doe42",
	"age": 34,
	"state": "OPENED"
}

db.mycollec42.save(o)