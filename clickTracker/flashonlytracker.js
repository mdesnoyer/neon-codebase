/* Script Instructions:
 *
 * For the player under test, go to publish settings and enable javascript api
 * If javascript api is not being used, then do the following
 <param name="includeAPI" value="true" />
 <param name="templateLoadHandler" value="NeonFlashTracker.onTemplateLoad" />
 <param name="templateReadyHandler" value="NeonFlashTracker.onTemplateReady" />

 * Else if <param name="templateLoadHandler" ... /> already is on the page, then open existing script file and add
 * this line at the end of onTemplateLoad method, NeonFlashTracker.hookNeonTrackerToFlashPlayer(expID);
 * where expID is the function argument variable name that's passed to the onTemplateLoad method.  
*/

var NeonTrackerURL = "http://localhost:8888";
var NeonTrackerType = "flashonlyplayer";
var NeonAccountID = NeonAccountID || "accountIDNotSet";
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
			var req = url + "?" + params + "&ts=" + ts + "&page=" + encodeURIComponent(pageURL) + "&aid=" + NeonAccountID + "&ttype=" + NeonTrackerType;
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
/// Neon Tracker for Brightcove Flash Players
var NeonFlashTracker = ( function () {
    var player, videoPlayer, content, exp, initialVideo;
    return {
        onTemplateLoad: function (expID){                                           
            console.log( "template loaded " + new Date().getTime());                 
			NeonFlashTracker.hookNeonTrackerToFlashPlayer(expID);
        },
                                                       
		hookNeonTrackerToFlashPlayer: function(expID) { 
			console.log("Hoooked Neon Tracker!");
            player = bcPlayer.getPlayer(expID);                                      
            videoPlayer = player.getModule(APIModules.VIDEO_PLAYER);                 
            content = player.getModule(APIModules.CONTENT);                         
			exp     = player.getModule(APIModules.EXPERIENCE); 
            videoPlayer.addEventListener(BCMediaEvent.BEGIN, NeonFlashTracker.onMediaBegin);
			exp.addEventListener(BCExperienceEvent.CONTENT_LOAD, NeonFlashTracker.trackLoadedImageUrls);
			//Test //setTimeout( function() { location.reload(); } , 5000);
		 },                             
 
        onTemplateReady: function (evt) {                                         
            console.log("template ready " + new Date().getTime());
			initialVideo = videoPlayer.getCurrentVideo();
			NeonFlashTracker.trackLoadedImageUrls();
		},
		
		trackLoadedImageUrls: function(){
            var imageUrls = new Array();
            var stillUrls = new Array();                                            
			var mediaCollection = content.getAllMediaCollections();
			console.log(mediaCollection);
            if (mediaCollection.length >0 && mediaCollection[0].mediaCount > 0){
            	for(var i = 0; i < mediaCollection[0].mediaCount; i++) {
				   	if (mediaCollection[0].mediaIds[i] != initialVideo.id){	
                		imageUrls[i] = content.getMedia(mediaCollection[0].mediaIds[i]) ["thumbnailURL"].split('?')[0]; 
                		stillUrls[i] = content.getMedia(mediaCollection[0].mediaIds[i]) ["videoStillURL"].split('?')[0];
					}
            	}
			}
			console.log(imageUrls);
			action = "load";
			params = "a=" + action + "&id="+ reqGuid + "&imgs=" + encodeURIComponent(JSON.stringify(imageUrls));
			NeonDataSender.sendRequest(NeonTrackerURL,params);			           
        },
                                                                                     
        onMediaBegin: function (evt) {
            console.log( "Media Begin ! " + new Date().getTime());            
		  	var vid    = evt.media.id;	
			var action = "click";
			var imgSrc = evt.media.thumbnailURL.split("?")[0]; //clean up
			//stillSrc = evt.media.videoStillURL;
			params = "a=" + action + "&id="+ reqGuid + "&img=" + encodeURIComponent(imgSrc);
		    if (vid != initialVideo.id){	
				NeonDataSender.sendRequest(NeonTrackerURL,params);			           
			}	
        }
    }
}());
