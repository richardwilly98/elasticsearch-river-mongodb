var status = rs.status()
for (i=0; i < status.members.length; i++) {
  if (status.members[i].state == 1) {
    db = connect(status.members[i].name + "/local")
  }
}
use mydb211
var modified = new Date()
db.media.update({"user": "joe.doe"}, {$set: {"deleted": true, "modified": modified}})
