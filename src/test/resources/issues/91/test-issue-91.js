use mydb91
var doc = db.fs.files.findOne()
db.fs.files.update({"_id": doc._id}, {$set: {"metadata.titleDoc":"test91"}})
