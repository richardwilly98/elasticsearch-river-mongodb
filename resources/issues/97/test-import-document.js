use mydb97
var student = {"name": "joe", "scores": [54]}
db.mycollec97.save(student)
db.mycollec97.update({"name": "joe"}, {$push: {"scores": 89}})
