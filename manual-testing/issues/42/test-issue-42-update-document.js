var status = rs.status()
 for (i=0; i < status.members.length; i++) {
   if (status.members[i].state == 1) {
     db = connect(status.members[i].name + "/local")
   }
 }

use mydb
var o = db.mycollec.findOne({"firstName": "John42", "lastName": "Doe42"})
o.state = 'CLOSED';
db.mycollec.save(o)