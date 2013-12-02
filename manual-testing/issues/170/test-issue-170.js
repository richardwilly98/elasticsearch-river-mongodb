var status = rs.status()
 for (i=0; i < status.members.length; i++) {
   if (status.members[i].state == 1) {
     db = connect(status.members[i].name + "/local")
   }
 }

var max = 1000000;
use mydb170
for (var i=0; i < max; i++) {
  var o = {
    "firstName": "John",
    "lastName": "Doe",
    "employeeId": i,
    "title": "Developer"
  }
  db.mycollection170.save(o);
  //print("save object " + o["_id"]);
}
