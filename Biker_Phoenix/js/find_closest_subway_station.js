var address1 = prompt("Enter the address:", "Penn Station");


function get_walk_time(address1,address2){

	var mode = "walking"

	if (address1 == null || address1 == "" || address2 == null || address2 == "") {
		alert("User cancelled one of the prompts");
	}
	else {
		var url_string = "https://maps.googleapis.com/maps/api/distancematrix/json?units=imperial&origins="+address1+"&destinations="+address2+"&mode="+mode+"&key=AIzaSyCI7fCvGW2y8fVb8SzohlAzFAhDZ0eJGsI";
	}

	var xmlHttp = new XMLHttpRequest();
	xmlHttp.open( "GET", url_string, false ); // false for synchronous request
	xmlHttp.send(null);
	var data = JSON.parse(xmlHttp.responseText);
	
	var time_seconds = data.rows[0].elements[0].duration.value;
	var time_minutes = parseFloat(time_seconds)/60; //time in minutes
	
	return time_minutes

}


function reverse_geocoding(latitude,longitude){
	var url_string = "https://maps.googleapis.com/maps/api/geocode/json?latlng=" + latitude +","+longitude+ "&key=AIzaSyD0y1Q1FGLwHEkqjPHrNeodwGCf3VRZYlA";
	var xmlHttp = new XMLHttpRequest();
	xmlHttp.open( "GET", url_string, false ); // false for synchronous request
	xmlHttp.send( null );
	var data = JSON.parse(xmlHttp.responseText);
	var address = data.results[0].formatted_address;
	return address;

function find_closest_subway_station(address){
	
	var latitude, var longitude = geocoding(address);

	var url = "/closestSubway/:" + latitude + "/:" + longitude;
    response = httpGet(url);

    console.log(response);

	var subway_station_id = response.id;
	var subway_station_latitude = response.latitude;
	var subway_station_longitude = response.longitude;

	//calculate walking time to address of the station found
	var subway_station_address = reverse_geocoding(subway_station_latitude,subway_station_longitude);
	var walking_time = get_walk_time(address,subway_station_address);
	return subway_station_id, walking_time;
}




a = find_closest_subway_station(address1);