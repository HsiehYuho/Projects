var express = require('express')
var passport = require('passport');
var Strategy = require('passport-facebook').Strategy;
var url = "mongodb://localhost:27017/mydb";

const Mongod = require('mongod');
const server = new Mongod(27017); 
server.open((err) => {
	if (err === null) {
		console.log('success connect to mongod');
	}
	else {
		console.log(err);
	}
});

// Retrieve
var MongoClient = require('mongodb').MongoClient;

var app = express()
var path = require('path');
app.use("/styles", express.static(__dirname + '/styles'));
app.use("/images", express.static(__dirname + '/images'));
app.use("/js", express.static(__dirname + '/js'));
app.use("/controllers", express.static(__dirname + '/controllers'));

/* Connection Here */
var mysql = require('mysql');
var connection = mysql.createConnection({
	host: 'biker.c09mjnvqwegg.us-east-1.rds.amazonaws.com',
	user: 'admin',
	password: '1qaz2wsx',
	database: 'dummy',
	port: '8000'
});

passport.use(new Strategy({
    clientID: 781763642029325,
    clientSecret: '4c76299f38ff88f356ed7ef1dd2b83f4',
    callbackURL: 'http://localhost:5000/login/facebook/return'
  },
  function(accessToken, refreshToken, profile, cb) {
    // In this example, the user's Facebook profile is supplied as the user
    // record.  In a production-quality application, the Facebook profile should
    // be associated with a user record in the application's database, which
    // allows for account linking and authentication with other identity
    // providers.
    return cb(null, profile);
  }));

passport.serializeUser(function(user, cb) {
  cb(null, user);
});

passport.deserializeUser(function(obj, cb) {
  cb(null, obj);
});
// Configure view engine to render EJS templates.

app.set('views', __dirname + '/view');
app.set('view engine', 'ejs');

// Use application-level middleware for common functionality, including
// logging, parsing, and session handling.
app.use(require('morgan')('combined'));
app.use(require('cookie-parser')());
app.use(require('body-parser').urlencoded({ extended: true }));
app.use(require('express-session')({ secret: 'keyboard cat', resave: true, saveUninitialized: true }));
app.use(passport.initialize());
app.use(passport.session());

app.set('port', (process.env.PORT || 5000))
app.use(express.static(__dirname + '/public'))

app.get('/', function(req, res, next) {
	if (req.user) {
		res.render('index', { user: req.user });
	} else {
		res.render('login');
	}
	
});

app.get('/guest', function(req, res, next) {
		console.log("HERE")
		res.render('index', {user: "Guest"});
	
});

app.get('/getname', function(req,res,next){
	if (req.user){
		res.send(req.user.displayName);
	}
	else {
		res.send("Guest");
	}
	console.log(req.user);
});


app.get('/map', function(req, res, next) {
	res.sendFile(path.join(__dirname, '/view', 'map.html'));
});

app.get('/user-history', function(req, res, next) {
	res.sendFile(path.join(__dirname, '/view', 'user-history.html'));
});

app.get('/bike-stations', function(req, res, next) {
	res.sendFile(path.join(__dirname, '/view', 'bike-stations.html'));
});

app.get('/subway-stations', function(req, res, next) {
	res.sendFile(path.join(__dirname, '/view', 'subway-stations.html'));
});

app.get('/all-bikes/:location', function(req, res) {

	MongoClient.connect(url, function(err, db) {
		if (err) throw err;
		var search = req.params.location;
		var query = {
			stationName: new RegExp(search, 'i')
		};
		var dbase = db.db("mydb");
		dbase.collection("bike").find(query).toArray(function(err, result) {
			if (err) throw err;
			res.send(result);
			db.close();
		});
	});
});

app.get('/all-subways/:location', function(req, res) {

	var query = "SELECT * FROM subway_stations WHERE name LIKE '%" + req.params.location + "%'";
	console.log(query);

	connection.query(query, function(err, rows, fields) {
		if (err) {
			console.log(err);
		}
		else {
			res.send(rows);
		}
	});
});

app.get('/cached-location-insert/:lat/:lon/:add', function(req, res) {

	var query = "INSERT INTO cachedLocations VALUES ('" + req.params.add + "', '" + req.params.lat + "', '" + req.params.lon + "');"
	console.log("INSERT!: " + query);

	connection.query(query, function(err, rows, fields) {
		if (err) {
			console.log(err);
		}
		else {
			console.log("Inserted: " + req.params.add);
			res.send(rows);
		}
	});
});

app.get('/cached-location-check/:location', function(req, res) {

	var query = "SELECT * FROM cachedLocations WHERE stringAddress = '" + req.params.location.split(' ').join('+') + "'";
	console.log("Checking Now: " + query);

	connection.query(query, function(err, rows, fields) {
		if (err) {
			console.log(err);
		}
		else {
			if (rows) {
				res.send(rows);
				console.log(rows);
			}
			else {
				console.log("No Response");
			}
		}
	});
});

app.get('/bestPath/:longitude/:latitude', function(req, res) {

	console.log("/bestPath/:" + req.params.longitude + "/:" + req.params.latitude);

	var query = "SELECT * FROM bike_stations";
	console.log(query);

	connection.query(query, function(err, rows, fields) {
		if (err) {
			console.log(err);
		}
		else {
			res.send(rows);
		}
	});
});

app.get('/closestSubway/:latitude/:longitude', function(req, res) {

	var query = 'SELECT s.id, s.longitude, s.latitude, s.officialAddress, s.name FROM subway_stations_official_names s ORDER BY  POWER((s.longitude -(' + req.params.longitude + ')),2)+POWER((s.latitude - (' + req.params.latitude + ')),2) ASC LIMIT 1';
	console.log(query);

	connection.query(query, function(err, rows, fields) {
		if (err) {
			console.log(err);
		}
		else {
			res.send(rows);
		}
	});
});

app.get('/closestBike/:latitude/:longitude', function(req, res) {

	var query = 'SELECT b.bike_station_id, b.longitude, b.latitude, b.officialAddress, b.stationName FROM bike_stations_official_names b ORDER BY  POWER((b.longitude -(' + req.params.longitude + ')),2)+POWER((b.latitude - (' + req.params.latitude + ')),2) ASC LIMIT 1';
	console.log(query);

	connection.query(query, function(err, rows, fields) {
		if (err) {
			console.log(err);
		}
		else {
			res.send(rows);
		}
	});
});

app.get('/closestBikeToSubway/:subway_id', function(req, res) {

	var query = 'SELECT b.bike_station_id, s.id, b.latitude as blat, b.longitude as blong, b.officialAddress as bAdd, s.latitude as slat, s.longitude as slong, s.officialAddress as sAdd FROM bike_stations_official_names b, subway_stations_official_names s WHERE b.bike_station_id = (SELECT bike_station_id FROM subway_bike_shortest_distances WHERE subway_station_id ="'+req.params.subway_id+'") AND s.id = "'+req.params.subway_id+'"';

	//var query = 'SELECT bike_station_id, latitude, longitude FROM bike_stations WHERE bike_station_id = (SELECT bike_station_id FROM subway_bike_shortest_distances WHERE subway_station_id = "'+req.params.subway_id+'")';

	//var query = 'SELECT bike_station_id, distance FROM subway_bike_shortest_distances WHERE subway_station_id ="'+ req.params.subway_id +'"';
	console.log(query);

	connection.query(query, function(err, rows, fields) {
		if (err) {
			console.log(err);
		}
		else {
			res.send(rows);
		}
	});
});

app.get('/all-history/', function(req, res) {


	if (req.user){
		var name =req.user.displayName;
	}
	else {
		var name = "guest";
	}

	var query = 'SELECT source, destination, time FROM Users WHERE username="'+name+'"';

	console.log(query);

	connection.query(query, function(err, rows, fields) {
		if (err) {
			console.log(err);
		}
		else {
			res.send(rows);
		}
	});
});


app.get('/add-history/:source/:destination', function(req, res) {

	var time = new Date();
	var now=(time.getHours() + ":" + time.getMinutes() + ":" + time.getSeconds());

	if (req.user){
		var name =req.user.displayName;
	}
	else {
		var name = "guest";
	}

	console.log(now)
	var query = 'Insert into Users VALUES("'+name+'","'+req.params.source+'","'+req.params.destination+'","'+now+'")';

	console.log(query);

	connection.query(query, function(err, rows, fields) {
		if (err) {
			console.log(err);
		}
		else {
			res.send(rows);
		}
	});
});





app.get('/login',
  function(req, res){
    res.render('trial');
  });

app.get('/login/facebook',
  passport.authenticate('facebook'));

app.get('/login/facebook/return', 
  passport.authenticate('facebook', { failureRedirect: '/login' }),
  function(req, res) {
    res.redirect('/');
  });

app.get('/profile',
  require('connect-ensure-login').ensureLoggedIn(),
  function(req, res){
    res.render('profile', { user: req.user });
  });

app.get('/logout', function (req, res){
  req.session.destroy(function (err) {
    res.redirect('/'); //Inside a callback… bulletproof!
  });
});

app.listen(app.get('port'), function() {
	console.log("Node app is running at localhost:" + app.get('port'))
});