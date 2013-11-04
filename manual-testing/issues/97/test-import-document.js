var status = rs.status()
for (i=0; i < status.members.length; i++) {
  if (status.members[i].state == 1) {
    db = connect(status.members[i].name + "/local")
  }
}
use mydb97
var student = {"name": "joe", "scores": [54]}
db.mycollec97.save(student)
db.mycollec97.update({"name": "joe"}, {$push: {"scores": 89}})
