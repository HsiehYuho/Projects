var MongoClient = require('mongodb').MongoClient;
var url = "mongodb://localhost:27017/mydb";

MongoClient.connect(url, function(err, db) {
  if (err) throw err;
  var dbase = db.db("mydb"); //here
  dbase.createCollection("bike", function(err, res) {
    if (err) throw err;
    console.log("Collection created!");
    db.close();
  });
});