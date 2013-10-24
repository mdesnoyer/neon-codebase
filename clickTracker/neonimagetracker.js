//var neonTrackerTestMode =1
var NeonTrackerType = "imagetracker";
var NeonDataSender = (function() {

	function JSONscriptRequest(fullUrl) {
		this.fullUrl = fullUrl; 
		this.noCacheIE = '&noCacheIE=' + (new Date()).getTime();
		this.headLoc = document.getElementsByTagName("head").item(0);
		this.scriptId = 'JscriptId' + JSONscriptRequest.scriptCounter++;
	}

	JSONscriptRequest.scriptCounter = 1;
	JSONscriptRequest.prototype.buildScriptTag = function () {
		this.scriptObj = document.createElement("script");
		this.scriptObj.setAttribute("type", "text/javascript");
		this.scriptObj.setAttribute("charset", "utf-8");
		this.scriptObj.setAttribute("src", this.fullUrl + this.noCacheIE);
		this.scriptObj.setAttribute("id", this.scriptId);
	};
	
	JSONscriptRequest.prototype.removeScriptTag = function () {
		this.headLoc.removeChild(this.scriptObj);  
	};
	
	JSONscriptRequest.prototype.addScriptTag = function () {
		this.headLoc.appendChild(this.scriptObj);
	}

	return{ 
		sendRequest: function(url, params){
			var pageURL = (document.URL).split('?')[0]; // Ignore any get params	
			var ts = new Date().getTime(); 
			var req = url + "?" + params + "&ts=" + ts + "&page=" + encodeURIComponent(pageURL) + "&ttype=" + NeonTrackerType;
			if ( typeof neonTrackerTestMode != 'undefined'){ 
				req = "http://localhost:8888/test" + "?" + params + "&ts=" + ts + "&page=" + encodeURIComponent(pageURL) + "&ttype=" + NeonTrackerType;
				req = req+"&callback=NeonImageTracker.testJsonCallback";}
			console.log("Send request to Neon " + req );
			try { bObj = new JSONscriptRequest(req); bObj.buildScriptTag(); bObj.addScriptTag();  } catch(err) {}	
		},

		_NeonPageRequestUUID: function(){
			function genRandomHexChars() {
				return Math.floor((1 + Math.random()) * 0x10000).toString(16).substring(1); 
			}
			return genRandomHexChars() + genRandomHexChars() + genRandomHexChars() + genRandomHexChars();
		}
	}
}());

var reqGuid = NeonDataSender._NeonPageRequestUUID();
var NeonImageTracker = ( function ()  {
    var NeonTrackerURL = "http://localhost:8888/track";
	
	return {
		testJsonCallback: function (jsonData){
			action = jsonData["a"];
			alert( "Image " + action + " works");
		},

	trackerInit: function () {
		$(document).ready(function () {
			$(window).ready(function(){
				var action = "load";
				var imgTags = document.getElementsByTagName("img");
				if (!imgTags) {
					imgTags = $(this).attr("img"); //use jquery
				}
				var imgs = new Array();
				for (var i = 0; i < imgTags.length; i++) {
					imgs.push(imgTags[i].src);
				}	
				params = "a=" + action + "&id="+ reqGuid + "&imgs=" + imgs;
				NeonDataSender.sendRequest(NeonTrackerURL,params);
		});
    	$("img").mousedown(function(e) {
	    	var action = "click";	
			var imgSrc = $(this).attr('src');
			var coordinates = e.pageX  + "," + e.pageY;
			params = "a=" + action + "&id="+ reqGuid + "&img=" + encodeURIComponent(imgSrc) + "&xy=" + coordinates; 
			NeonDataSender.sendRequest(NeonTrackerURL,params);
	  	}); 
	});
	}
   
}
}());

// Only do anything if jQuery isn't defined
if (typeof jQuery == 'undefined') {

	if (typeof $ == 'function') {
		var thisPageUsingOtherJSLibrary = true;
	}

	//get script 		
	function getScript(url, success) {
	
		var script     = document.createElement('script');
		script.src = url;
		var head = document.getElementsByTagName('head')[0],
		done = false;
		// Attach handlers for all browsers
		script.onload = script.onreadystatechange = function() {
		if (!done && (!this.readyState || this.readyState == 'loaded' || this.readyState == 'complete')) {
				done = true;
				// callback function provided as param
				success();
				script.onload = script.onreadystatechange = null;
				head.removeChild(script);
			};
		};
		head.appendChild(script);
	
	};
	console.log("missing jquery");
	getScript('http://ajax.googleapis.com/ajax/libs/jquery/1.3.1/jquery.min.js', function() {
	if (typeof jQuery=='undefined') {
			console.log("JQuery loading failed even after explicit load call");
		} else {
				// check if other conflicting library is present
				// try { $.noConflict(true); } catch(err) {} 
				NeonImageTracker.trackerInit();
		}
	});
	
} else { 
	// jQuery was already loaded
	NeonImageTracker.trackerInit();
};
