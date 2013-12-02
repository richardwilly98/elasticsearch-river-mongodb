var status = rs.status()
 for (i=0; i < status.members.length; i++) {
   if (status.members[i].state == 1) {
     db = connect(status.members[i].name + "/local")
   }
 }

use mydb42
var o = db.mycollec42.findOne({"firstName": "John42", "lastName": "Doe42"})
o.state = 'CLOSED';
db.mycollec42.save(o)