var adminDb;
function createReplicaset() {
	var config = { _id: "replica1", members:[{ _id : 0, host : "localhost:27017" }, { _id : 1, host : "localhost:27018" }, { _id : 2, host : "localhost:27019" }]};
	var cmd = {"replSetInitiate": config};
	adminDb.runCommand(cmd);
	//rs.status();
}
adminDb = db.getSiblingDB("admin");
createReplicaset();