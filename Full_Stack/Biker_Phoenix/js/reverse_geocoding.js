

var latitude = prompt("Enter the latitude:", "40.714224");
var longitude = prompt("Enter the longitude:", "-73.961452");

if (latitude == null || latitude == "" || longitude == null || longitude == "") {
	alert("User cancelled the prompt");
}

else {
	var url_string = "https://maps.googleapis.com/maps/api/geocode/json?latlng=" + latitude +","+longitude+ "&key=AIzaSyD0y1Q1FGLwHEkqjPHrNeodwGCf3VRZYlA";
}


var xmlHttp = new XMLHttpRequest();
xmlHttp.open( "GET", url_string, false ); // false for synchronous request
xmlHttp.send( null );
var data = JSON.parse(xmlHttp.responseText);

str = data.results[0].formatted_address;

alert(str);