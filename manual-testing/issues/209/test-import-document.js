var status = rs.status()
for (var i=0; i < status.members.length; i++) {
  if (status.members[i].state == 1) {
    db = connect(status.members[i].name + "/local")
  }
}
use mydb209
// db = db.getMongo().getDB( "mydb209" );

var count = db.mycollec209.count()
print('count: ' + count)
var max = count + 1000000
for (var i=count+1; i <= max; i++) {
  var created = new Date()
  var item = {"user": "joe.doe-" + i, "scores": [count], "deleted": false, "created": created}
  // print(item);
  db.mycollec209.save(item)
}