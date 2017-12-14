# Using different colors to dipslay different sections of route on google map 

## Introduction


### How to run it?
Download all three files and double click map.html file.
You can choose different types of communting. 
After click summit, different color of line means different types of commuting. 

## Code part

There are details comment in map.js file. 
Just mention some important parts of the code.

### Type of commuting 
I design 5 types of commuting, you can change the color whatever you want.

WALKING1, WALKING2,BIKING1,BIKING2,SUBWAY 
WALKING1 means the first time of walk
WALKING2 means the second time of walk
BIKING1 means the first time of bike
BIKING2 means the second time of bike

There will only be 1 time of subway.

### Type of paramter
Please pass the parameter to the variable routes in the following format:

'''
routes = [[start1,end1,type1],[start2,end2,type2],[start3,end3,type3]].
'''

The end1 should be the same as start2, and end2 should be the same as start3.
However, they can still be different. 

### Where should change?
* In the below section, we need to change the value of route to link the different pages. (Use the result of get_fast_path functions)

```
    clear_all();

	var start = document.getElementById('start').value;
	var mid = document.getElementById('mid').value;
	var end = document.getElementById('end').value;
	var type1 = document.getElementById('type1').value;
	var type2 = document.getElementById('type2').value;
	routes = [[start,mid,type1],[mid,end,type2]];

	clicke();
```
* Google API key for google api direction service 

![Alt text](./pics/pic1.png?raw=true "DEMO1")
![Alt text](./pics/pic2.png?raw=true "DEMO2")
