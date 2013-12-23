var status = rs.status()
 for (i=0; i < status.members.length; i++) {
   if (status.members[i].state == 1) {
     db = connect(status.members[i].name + "/local")
   }
 }

function getRandomInt (min, max) {
    return Math.floor(Math.random() * (max - min + 1)) + min;
}

use mydb1900

for (var i=0; i < 20; i++) {
  var orderA =
  {
    "name": "orderA-" + i,
    "cust_id": getRandomInt(0, 20),
    "amount": getRandomInt(1, 500),
    "status": "A"
  }
  db.orders.save(orderA)
  var orderB =
  {
    "name": "orderB-" + i,
    "cust_id": getRandomInt(0, 20),
    "amount": getRandomInt(1, 500),
    "status": "B"
  }
  db.orders.save(orderB)
}

db.orders.mapReduce(
  function() {
    emit(this.cust_id, this.amount);
  },
  function (key, values) {
    return Array.sum( values )
  },
  {
    "query": {"status": "A"},
    "out": "order_totals"
  }
)