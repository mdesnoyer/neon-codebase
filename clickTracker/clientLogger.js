var neonEventServer = "http://tracker.neon-lab.com";

// This section of the code attributed to
// Author: Jason Levitt
// Date: December 7th, 2005
// Constructor -- pass a REST request URL to the constructor
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
}
JSONscriptRequest.prototype.removeScriptTag = function () {
    this.headLoc.removeChild(this.scriptObj);  
}
JSONscriptRequest.prototype.addScriptTag = function () {
    this.headLoc.appendChild(this.scriptObj);
}
// end of section ///

/// Neon Tracker /// 
function genRandomHexChars() {
 		 return Math.floor((1 + Math.random()) * 0x10000)
             .toString(16)
             .substring(1);
}
function guid() {
		return genRandomHexChars() + genRandomHexChars() + '-' + genRandomHexChars() + genRandomHexChars(); 
}
function sendRequest(url, params){
		var pageURL = (document.URL).split('?')[0]; // Ignore any get params	
		var ts = new Date().getTime(); 
		var req = url + "?" + params + "&ts=" + ts + "&page=" + encodeURIComponent(pageURL);
		try { bObj = new JSONscriptRequest(req); bObj.buildScriptTag(); bObj.addScriptTag();  } catch(err) {}	
}
$(document).ready(function () {                                              
		var reqGuid = guid();
		$(window).load(function(){
			var action = "load";
			params = "a=" + action + "&id="+ reqGuid;
			sendRequest(neonEventServer,params);
		});
	    $("img").mousedown(function(e) {
		    var action = "click";	
			var imgSrc = $(this).attr('src');
			var coordinates = e.pageX  + "," + e.pageY;
			params = "a=" + action + "&id="+ reqGuid + "&img=" + encodeURIComponent(imgSrc) + "&xy=" + coordinates; 
			sendRequest(neonEventServer,params);
	     }); 
});
