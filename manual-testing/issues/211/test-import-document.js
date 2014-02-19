var status = rs.status()
for (i=0; i < status.members.length; i++) {
  if (status.members[i].state == 1) {
    db = connect(status.members[i].name + "/local")
  }
}
use mydb211
var created = new Date()
var item = {"user": "joe.doe", "scores": [54], "deleted": false, "created": created, "modified": created, "meta": ["tag1", "tag2"], "status": "A", "dummy": 1}
db.media.save(item)
