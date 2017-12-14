function index_of_min(array) {
    var index = 0;
	var value = array[0];
	for (var i = 1; i < array.length; i++) {
	  if (array[i] < value) {
	    value = array[i];
	    index = i;
	  }
	}
	return index;
}

function httpGet(url) {
    var xmlHttp = new XMLHttpRequest();
    xmlHttp.open( "GET", url, false ); // false for synchronous request
    xmlHttp.send( null );
    return xmlHttp.responseText;
}

function update_address(address){
    address=address.replace(" ","+");
    return address;
}

function find_closest_subway_station(address){
	
	response = geocoding(address);
	latitude = response[0]
	longitude = response[1]

	var url = "../closestSubway/" + latitude + "/" + longitude;
    response2 = httpGet(url);
    response2 = eval(response2)[0];

	var subway_station_id = response2.id;
	var subway_station_latitude = response2.latitude;
	var subway_station_longitude = response2.longitude;
	var subway_station_address = response2.officialAddress;
	var subway_station_display_address = response2.name;

	//calculate walking time to address of the station found
	//var subway_station_address = reverse_geocoding(subway_station_latitude,subway_station_longitude);

	var walking = get_walk_time(address,subway_station_address);
	walking_time = walking[0];
	walking_distance = walking[1];

	return [subway_station_id, subway_station_address, walking_time, walking_distance, subway_station_display_address];
}

function find_closest_bike_station(address){

	address = update_address(address);

	response = geocoding(address);

	latitude = response[0];
	longitude = response[1];
	//here goes SQL query that returns closes bike_station_id, its latitude and longitude
	url = '/closestBike/'+latitude+'/'+longitude;
	response2 = httpGet(url);
	
	response2 = eval(response2);
	//response2 = JSON.parse(response2);
	

	var bike_station_id = response2[0].bike_station_id;
	var bike_station_latitude = response2[0].latitude;
	var bike_station_longitude = response2[0].longitude;
	var bike_station_address = response2[0].officialAddress;
	var bike_station_display_address = response2[0].stationName;

	//calculate walking time to address of the station found

	//var bike_station_address = reverse_geocoding(bike_station_latitude,bike_station_longitude);
	var walking = get_walk_time(address,bike_station_address);

	walking_time = walking[0];
	walking_distance = walking[1];


	return [bike_station_id, bike_station_address, walking_time, walking_distance, bike_station_display_address];
}

function find_closest_bike_to_subway(subway_station_id){

	//here goes SQL query that returns closes bike_station_id, its latitude and longitude
	url = '/closestBikeToSubway/'+subway_station_id;
	response2 = httpGet(url);
	
	response2 = eval(response2);

	bike_station_id = response2[0].bike_station_id;
	bike_station_latitude = response2[0].blat;
	bike_station_longitude = response2[0].blong;
	address_bike = response2[0].bAdd;

	subway_station_latitude = response2[0].slat;
	subway_station_longitude = response2[0].slong;
	address_subway = response2[0].sAdd;

	//address_bike = reverse_geocoding(bike_station_latitude,bike_station_longitude);
	//address_subway = reverse_geocoding(subway_station_latitude,subway_station_longitude);

	bike_to_subway = get_walk_time(address_bike,address_subway);
	walk_time_bike_to_subway = bike_to_subway[0];
	walk_distance_bike_to_subway = bike_to_subway[1];
	
	return [bike_station_id, address_bike, walk_time_bike_to_subway, walk_distance_bike_to_subway];
}

function geocoding(address){
	console.log("Being executed: geocoding");

	var cacheURL = '/cached-location-check/'+address;
	var resultCache = httpGet(cacheURL);

	if (resultCache != "[]") {
		// This function executes when the result has a match
		console.log("Request - [FOUND IN CACHE]: " + address + ". API call dismissed.");
		response2 = eval(resultCache);
		return [response2[0].latitude, response2[0].longitude];
	}

	else {

		console.log("Request - [Not cached, reaching Google]: " + address + ". Coordinated stored in cache");
		address = update_address(address);
		var url_string = "https://maps.googleapis.com/maps/api/geocode/json?address=" + address + "&key=AIzaSyD0y1Q1FGLwHEkqjPHrNeodwGCf3VRZYlA";
		var xmlHttp = new XMLHttpRequest();
		xmlHttp.open( "GET", url_string, false ); // false for synchronous request
		xmlHttp.send( null );
		var data = JSON.parse(xmlHttp.responseText);
		var latitude = data.results[0].geometry.location.lat;
		var longitude = data.results[0].geometry.location.lng;

		insertCacheURL = '/cached-location-insert/'+ latitude + '/' + longitude + '/' + address;
		httpGet(insertCacheURL);

		return [latitude, longitude];
	}
}

function reverse_geocoding(latitude,longitude){
	console.log("Being executed: reverse_geocoding");

	var url_string = "https://maps.googleapis.com/maps/api/geocode/json?latlng=" + latitude +","+longitude+ "&key=AIzaSyD0y1Q1FGLwHEkqjPHrNeodwGCf3VRZYlA";
	var xmlHttp = new XMLHttpRequest();
	xmlHttp.open( "GET", url_string, false ); // false for synchronous request
	xmlHttp.send( null );
	var data = JSON.parse(xmlHttp.responseText);
	var address = data.results[0].formatted_address;
	return address;
}

function get_walk_time(address1,address2){

	address1 = update_address(address1);
	address2 = update_address(address2);

	var mode = "walking"

	if (address1 == null || address1 == "" || address2 == null || address2 == "") {
		alert("User cancelled one of the prompts");
	}
	else {
		var url_string = "https://maps.googleapis.com/maps/api/distancematrix/json?units=imperial&origins="+address1+"&destinations="+address2+"&mode="+mode+"&key=AIzaSyCI7fCvGW2y8fVb8SzohlAzFAhDZ0eJGsI";
	}

	var data = httpGet(url_string);
	var data = eval(JSON.parse(data));
	
	var time_seconds = data.rows[0].elements[0].duration.value;
	var time_minutes = parseFloat(time_seconds)/60; //time in minutes

	var distance_meters = data.rows[0].elements[0].distance.value;
	var distance_km = parseFloat(distance_meters)/1000; //time in minutes
	
	return [time_minutes,distance_km];

}

function get_bike_time(address1,address2){

	address1 = update_address(address1);
	address2 = update_address(address2);

	var mode = "bicycling"

	if (address1 == null || address1 == "" || address2 == null || address2 == "") {
		alert("User cancelled one of the prompts");
	}
	else {
		var url_string = "https://maps.googleapis.com/maps/api/distancematrix/json?units=imperial&origins="+address1+"&destinations="+address2+"&mode="+mode+"&key=AIzaSyCI7fCvGW2y8fVb8SzohlAzFAhDZ0eJGsI";
	}

	var data = httpGet(url_string);
	var data = eval(JSON.parse(data));
	
	var time_seconds = data.rows[0].elements[0].duration.value;
	var time_minutes = parseFloat(time_seconds)/60; //time in minutes

	var distance_meters = data.rows[0].elements[0].distance.value;
	var distance_km = parseFloat(distance_meters)/1000; //time in minutes
	
	return [time_minutes,distance_km];

}

function get_subway_time(address1,address2){

	address1 = update_address(address1);
	address2 = update_address(address2);

	var mode = "transit&transit_mode=subway"

	if (address1 == null || address1 == "" || address2 == null || address2 == "") {
		alert("User cancelled one of the prompts");
	}
	else {
		var url_string = "https://maps.googleapis.com/maps/api/distancematrix/json?units=imperial&origins="+address1+"&destinations="+address2+"&mode="+mode+"&key=AIzaSyCI7fCvGW2y8fVb8SzohlAzFAhDZ0eJGsI";
	}

	var data = httpGet(url_string);
	var data = eval(JSON.parse(data));
	
	var time_seconds = data.rows[0].elements[0].duration.value;
	var time_minutes = parseFloat(time_seconds)/60; //time in minutes

	var distance_meters = data.rows[0].elements[0].distance.value;
	var distance_km = parseFloat(distance_meters)/1000; //time in minutes
	
	return [time_minutes,distance_km];

}




function get_best_path(address_1,address_2){

	step1 = Date.now();

	address1 = update_address(address_1);





	address2 = update_address(address_2);

	walk_only = get_walk_time(address1,address2);
	time_walk_only = walk_only[0];
	distance_walk_only = walk_only[1];

	//latitude_start, longitude_start = geocoding(address1);
	//latitude_end, longitude_end = geocoding(address2);


	response1 = find_closest_subway_station(address1);

	near_subway_station_start_id = response1[0];
	near_subway_station_start_address = response1[1];
	near_subway_start_walktime = response1[2];
	near_subway_start_walkdistance = response1[3];
	near_subway_station_start_display_address = response1[4];


	response2 = find_closest_subway_station(address2);

	near_subway_station_end_id = response2[0];
	near_subway_station_end_address = response2[1];
	near_subway_end_walktime = response2[2];
	near_subway_end_walkdistance = response2[3];
	near_subway_station_end_display_address = response2[4];


	response3 = find_closest_bike_station(address1);

	near_bike_station_start_id = response3[0];
	near_bike_station_start_address = response3[1];
	near_bike_start_walktime = response3[2];
	near_bike_start_walkdistance = response3[3];
	near_bike_station_start_display_address = response3[4];


	response4 = find_closest_bike_station(address2);

	near_bike_station_end_id = response4[0];
	near_bike_station_end_address = response4[1];
	near_bike_end_walktime = response4[2];
	near_bike_end_walkdistance = response4[3];
	near_bike_station_end_display_address = response4[4];


	bike = get_bike_time(near_bike_station_start_address,near_bike_station_end_address);
	bike_time = bike[0];
	bike_distance = bike[1];

	subway = get_subway_time(near_subway_station_start_address,near_subway_station_end_address);
	subway_time = subway[0];
	subway_distance = subway[1];

	
	response5 = find_closest_bike_to_subway(near_subway_station_start_id);

	bike_near_subway_start_address = response5[1];
	walk_from_bike_to_subway_time = response5[2];
	walk_from_bike_to_subway_distance = response5[3];

	response6 = find_closest_bike_to_subway(near_subway_station_end_id);

	bike_near_subway_end_address = response6[1];
	walk_from_subway_to_bike_time = response6[2];
	walk_from_subway_to_bike_distance = response6[3];

	bike_start = get_bike_time(near_bike_station_start_address,bike_near_subway_start_address);
	bike_time_start = bike_start[0]
	bike_distance_start = bike_start[1]

	bike_end = get_bike_time(near_bike_station_end_address,bike_near_subway_end_address);
	bike_time_end = bike_end[0]
	bike_distance_end = bike_end[1]


	//NOW WE COMPUTE THE TRAVEL TIME FOR EACH MODE

	total_time_walk = time_walk_only;
	walk_section_1 = [address_1,address_2, "WALKING1", time_walk_only];
	walk_stops_addresses = [walk_section_1]

	total_time_bike = near_bike_start_walktime + bike_time + near_bike_end_walktime;
	bike_section_1 = [address_1,near_bike_station_start_address,"WALKING1",near_bike_start_walktime,near_bike_start_walkdistance];
	bike_section_2 = [near_bike_station_start_address,near_bike_station_end_address,"BIKING1",bike_time, bike_distance];
	bike_section_3 = [near_bike_station_end_address,address_2,"WALKING4",near_bike_end_walktime,near_bike_end_walkdistance];
	bike_stops_addresses = [bike_section_1,bike_section_2,bike_section_3];

	total_time_subway = near_subway_start_walktime + subway_time + near_subway_end_walktime;
	subway_section_1 = [address_1,near_subway_station_start_address,"WALKING1",near_subway_start_walktime,near_subway_start_walkdistance];
	subway_section_2 = [near_subway_station_start_address,near_subway_station_end_address,"SUBWAY1",subway_time,subway_distance];
	subway_section_3 = [near_subway_station_end_address,address_2,"WALKING4",near_subway_end_walktime,near_subway_end_walkdistance];
	subway_stops_addresses = [subway_section_1,subway_section_2,subway_section_3];

	total_time_bike_subway = near_bike_start_walktime + bike_time_start + walk_from_bike_to_subway_time + subway_time + near_subway_end_walktime;
	bike_subway_section_1 = [address_1,near_bike_station_start_address,"WALKING1",near_bike_start_walktime,near_bike_start_walkdistance];
	bike_subway_section_2 = [near_bike_station_start_address,bike_near_subway_start_address,"BIKING1",bike_time_start,bike_distance_start];
	bike_subway_section_3 = [bike_near_subway_start_address,near_subway_station_start_address,"WALKING2",walk_from_bike_to_subway_time,walk_from_bike_to_subway_distance];
	bike_subway_section_4 = [near_subway_station_start_address,near_subway_station_end_address,"SUBWAY1",subway_time, subway_distance];
	bike_subway_section_5 = [near_subway_station_end_address,address_2,"WALKING4",near_subway_end_walktime,near_subway_end_walkdistance];
	bike_subway_stops_addresses = [bike_subway_section_1,bike_subway_section_2,bike_subway_section_3,bike_subway_section_4,bike_subway_section_5];

	total_time_subway_bike = near_subway_start_walktime + subway_time + walk_from_subway_to_bike_time + bike_time_end + near_bike_end_walktime;
	subway_bike_section_1 = [address_1,near_subway_station_start_address,"WALKING1",near_subway_start_walktime, near_subway_start_walkdistance];
	subway_bike_section_2 = [near_subway_station_start_address,near_subway_station_end_address,"SUBWAY1",subway_time,subway_distance];
	subway_bike_section_3 = [near_subway_station_end_address,bike_near_subway_end_address,"WALKING3",walk_from_subway_to_bike_time,walk_from_subway_to_bike_distance];
	subway_bike_section_4 = [bike_near_subway_end_address,near_bike_station_end_address,"BIKING2",bike_time_end, bike_distance_end];
	subway_bike_section_5 = [near_bike_station_end_address,address_2,"WALKING4",near_bike_end_walktime, near_bike_end_walkdistance];
	subway_bike_stops_addresses = [subway_bike_section_1,subway_bike_section_2,subway_bike_section_3,subway_bike_section_4,subway_bike_section_5];

	total_time_bike_subway_bike = near_bike_start_walktime + bike_time_start + walk_from_bike_to_subway_time + subway_time + walk_from_subway_to_bike_time + bike_time_end + near_bike_end_walktime;
	bike_subway_bike_section_1 = [address_1,near_bike_station_start_address,"WALKING1",near_bike_start_walktime, near_bike_start_walkdistance];
	bike_subway_bike_section_2 = [near_bike_station_start_address,bike_near_subway_start_address,"BIKING1",bike_time_start, bike_distance_start];
	bike_subway_bike_section_3 = [bike_near_subway_start_address,near_subway_station_start_address,"WALKING2",walk_from_bike_to_subway_time, walk_from_bike_to_subway_distance];
	bike_subway_bike_section_4 = [near_subway_station_start_address,near_subway_station_end_address,"SUBWAY1",subway_time, subway_distance];
	bike_subway_bike_section_5 = [near_subway_station_end_address,bike_near_subway_end_address,"WALKING3",walk_from_subway_to_bike_time,walk_from_subway_to_bike_distance];
	bike_subway_bike_section_6 = [bike_near_subway_end_address,near_bike_station_end_address,"BIKING2",bike_time_end,bike_distance_end];
	bike_subway_bike_section_7 = [near_bike_station_end_address,address_2,"WALKING4",near_bike_end_walktime, near_bike_end_walkdistance];
	bike_subway_bike_stops_addresses = [bike_subway_bike_section_1,bike_subway_bike_section_2,bike_subway_bike_section_3,bike_subway_bike_section_4,bike_subway_bike_section_5,bike_subway_bike_section_6,bike_subway_bike_section_7];

	times = [total_time_walk, total_time_bike, total_time_subway, total_time_bike_subway, total_time_subway_bike, total_time_bike_subway_bike];
	modes = ["Walk", "Bike", "Subway", "Bike and Subway", "Subway and Bike", "Bike, Subway and Bike"];
	all_stops = [walk_stops_addresses, bike_stops_addresses, subway_stops_addresses, bike_subway_stops_addresses, subway_bike_stops_addresses, bike_subway_bike_stops_addresses];

	index = index_of_min(times);

	mode = modes[index];
	time = times[index];
	stops = all_stops[index];

	output = [mode, time, stops];

	step2 = Date.now();
	elapsed = (step2 - step1);
	console.log("Entire process took " + elapsed + " miliseconds");

	return output;

}

// var address1 = prompt("Enter the address:", "Grand Central Station");
// var address2 = prompt("Enter the address:", "Hotel Chantelle");

// if (address1 == null || address1 == "" || address2 == null || address2 == "") {
// 	alert("User cancelled the prompt");
// }

// else {
// 	response = get_best_path(address1,address2);
// }


// var xmlHttp = new XMLHttpRequest();
// xmlHttp.open( "GET", url_string, false ); // false for synchronous request
// xmlHttp.send( null );
// var data1 = station_id;
// var data2 = walking_time;

// alert(response[0]);
// alert(response[1]);
// alert(response[2]);


