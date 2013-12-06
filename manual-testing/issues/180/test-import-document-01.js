var status = rs.status()
 for (i=0; i < status.members.length; i++) {
   if (status.members[i].state == 1) {
     db = connect(status.members[i].name + "/local")
   }
 }

use mydb180
var o =
{
	"firstName": "John",
	"lastName": "Doe",
	"created": new Date()
}

db.mycollec180.save(o)