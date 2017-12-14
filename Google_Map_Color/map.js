// TEST PARAMS
// google.maps.event.addDomListener(window, "load", initMap(center_lat,center_lng));
	
// Global variables

// the information of different markers routes = [A,B,mode] 
// loc_to_coord = place -> lat,lng 'New York University' -> 1.234, 2.345
// locations -> the intermediate points ['New York University','Oscar Health'...]
// map_opt -> mapping mode to its belong display structure, which contains mode, directions service, pathline option  

var routes = [];
var locations = []; 
var loc_to_coord = new Map();
var map_opt = new Map();

var map;

// unused
var infowindow;

// initialize the map function
function initMap() {

	// NY garden
	var center_lat = 40.783412;
	var center_lng = -73.965954;
	
	// routes = [
	// 	['Time Square NY','Pennsylvania Station NY','WALKING1'],
	// 	['Pennsylvania Station NY','Prince Street Station NY', 'SUBWAY'],
	// 	['Prince Street Station NY','New Museum 235 Bowery NY', 'WALKING2']
	// ];

	// Set the walking route line - shallow-blue
  	var pathLine_w_1 = new google.maps.Polyline({
  		strokeColor: '#00ff00'
  	});
  	var pathLine_w_2 = new google.maps.Polyline({
  		strokeColor: '#00ff00'
  	});

  	// Set the biking route line - red
  	var pathLine_b_1 = new google.maps.Polyline({
  		strokeColor: '#FF0000'
  	});

  	// Set the biking route line - red
  	var pathLine_b_2 = new google.maps.Polyline({
  		strokeColor: '#FF0000'
  	});

  	// Set the subway route line - deep-blue
  	var pathLine_s = new google.maps.Polyline({
  		strokeColor: '#0000ff'
  	});

  	// initialize the display object
  	var dir_ser_w_1 = new google.maps.DirectionsService();
  	var dir_ser_w_2 = new google.maps.DirectionsService();
  	var dir_ser_b_1 = new google.maps.DirectionsService();
  	var dir_ser_b_2 = new google.maps.DirectionsService();
  	var dir_ser_s = new google.maps.DirectionsService();

  	var walk_obj_1 = {mode: 'WALKING',dir_ser: dir_ser_w_1, path_line: pathLine_w_1};
  	var walk_obj_2 = {mode: 'WALKING',dir_ser: dir_ser_w_2, path_line: pathLine_w_2};
  	var bike_obj_1 = {mode: 'BICYCLING',dir_ser: dir_ser_b_1, path_line: pathLine_b_1};
  	var bike_obj_2 = {mode: 'BICYCLING',dir_ser: dir_ser_b_2, path_line: pathLine_b_2};
  	var sub_obj = {mode: 'TRANSIT',dir_ser: dir_ser_s, path_line: pathLine_s};
  	
  	map_opt.set('WALKING1',walk_obj_1);
  	map_opt.set('WALKING2',walk_obj_2);
  	map_opt.set('BIKING1',bike_obj_1);
  	map_opt.set('BIKING2',bike_obj_2);
  	map_opt.set('SUBWAY',sub_obj);

	
	// add the map the set the map options
  	var centerOfMap = {
  		lat: center_lat,
 	 	lng: center_lng
  	};
  	map = new google.maps.Map(document.getElementById('map'), {
  		// 1 for earth, 10 for city, 15 for street
  		center: centerOfMap,
  		zoom: 12
  	});

  	// set event listener 
  	// the place we should change the element value to link the function change
    document.getElementById('submit').addEventListener('click', function() {
    	clear_all();

		var start = document.getElementById('start').value;
		var mid = document.getElementById('mid').value;
		var end = document.getElementById('end').value;
		var type1 = document.getElementById('type1').value;
		var type2 = document.getElementById('type2').value;
		routes = [[start,mid,type1],[mid,end,type2]];

		clicke();
    });

}


function clicke(){
  	// Set-up information
	init_location(routes);

    // Draw the markers
    setMarkers(map);

    // Draw path
    // console.log(routes[0]);
    for(i = 0; i < routes.length; i++){
	    draw_path(routes[i][0],routes[i][1],routes[i][2]);
    }

	console.log("FINISH");
}

function clear_all(){
	routes = [];
	locations = []; 
	loc_to_coord = new Map();
	initMap();
}

function init_location(routes){
	// push the very first position
	var set = new Set();
	for (i = 0; i < routes.length; i++){
		set.add(routes[i][0]);		
		set.add(routes[i][1]);		
	}
	for (let item of set){
		locations.push(item);
	}
	for(i = 0; i < locations.length; i++){
		var coord = address_to_coord(locations[i]);
		loc_to_coord.set(locations[i],coord);		
	}
}


function setMarkers(map){
	
	// Create bounds object
	var bounds = [];

	// place the markers on the map
	for(i = 0; i < locations.length; i++){
		var coord = loc_to_coord.get(locations[i]);
		var marker = new google.maps.Marker({
			position: {lat: coord[0],lng: coord[1]},
  			icon: 'http://maps.google.com/mapfiles/ms/micons/orange-dot.png',
  			map: map
		});
		var bound = new google.maps.LatLng(coord[0], coord[1]);
		bounds.push(bound);
	}

	// auto zoom 
	var lat_lng_bounds = new google.maps.LatLngBounds();
	for (i = 0; i < bounds.length; i++){
		lat_lng_bounds.extend(bounds[i]);
	}
	map.fitBounds(lat_lng_bounds);
	return;
}

// draw the path according to the mode 
// input start, end: string of address
// mode is either 'WALKING1','WALKING2','BIKING1','BIKING2' or 'SUBWAY'

function draw_path(start,end,mode){
	start_coord = loc_to_coord.get(start);
	end_coord = loc_to_coord.get(end);

	var loc_start = new google.maps.LatLng(start_coord[0], start_coord[1]);
	var loc_end = new google.maps.LatLng(end_coord[0], end_coord[1]);

	// choose the correct display object
	var obj = map_opt.get(mode);
	var direction_serive = obj.dir_ser;
	var path_line = obj.path_line;

	var request = {
		origin: loc_start, 
		destination: loc_end,
		optimizeWaypoints: false,
		travelMode: obj.mode
	};
	direction_serive.route(request, function(response, status) {
		if (status == google.maps.DirectionsStatus.OK) {
			var directions_display = new google.maps.DirectionsRenderer({
				suppressMarkers: true, 
				suppressBicyclingLayer: true,
				polylineOptions: path_line
			});
			directions_display.setMap(map);
			directions_display.setDirections(response);
        } 
    }); 

}


function address_to_coord(address) {
	if (address == null || address == "") {
		alert("Address invalid");
	}
	else {
		var url_string = "https://maps.googleapis.com/maps/api/geocode/json?address=" + address + "&key=YOUR_GOOGLE_API_KEY";
	}
	var xmlHttp = new XMLHttpRequest();
	xmlHttp.open( "GET", url_string, false ); // false for synchronous request
	xmlHttp.send( null );
	obj = JSON.parse(xmlHttp.responseText);
	// if(obj.status === 'ok')
	// console.log(obj.results.status);
	var lat = obj.results[0].geometry.location.lat
	var lng = obj.results[0].geometry.location.lng;
	return [lat,lng];
}

