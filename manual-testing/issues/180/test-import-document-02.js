var status = rs.status()
 for (i=0; i < status.members.length; i++) {
   if (status.members[i].state == 1) {
     db = connect(status.members[i].name + "/local")
   }
 }

use mydb180
var o = db.mycollec180.findOne()
o.score = 100;

db.mycollec180.save(o)