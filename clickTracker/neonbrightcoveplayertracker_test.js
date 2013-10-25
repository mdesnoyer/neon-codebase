var NeonTrackerType = "flashonlyplayer";
var PageLoadIDSeen = null; 
var MediaPlayPageIDSeen = null;
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

	var testMode = null;
	return{ 
		sendRequest: function(url, params){
			var pageURL = (document.URL).split('?')[0]; // Ignore any get params	
			var ts = new Date().getTime(); 
			var req = url + "?" + params + "&ts=" + ts + "&page=" + encodeURIComponent(pageURL) + "&ttype=" + NeonTrackerType;
			if (testMode){ req = req+"&callback=NeonPlayerTracker.testJsonCallback";}
			console.log("Send request to Neon " + req );
			try { bObj = new JSONscriptRequest(req); bObj.buildScriptTag(); bObj.addScriptTag();  } catch(err) {}	
		},

		_NeonPageRequestUUID: function(){
			function genRandomHexChars() {
				return Math.floor((1 + Math.random()) * 0x10000).toString(16).substring(1); 
			}
			return genRandomHexChars() + genRandomHexChars() + genRandomHexChars() + genRandomHexChars();
		},
		setTestMode: function(){
			testMode = 1;
		}
	}
}());

var reqGuid = NeonDataSender._NeonPageRequestUUID();
/// Neon Tracker for Brightcove Flash Players
var NeonPlayerTracker = ( function () {
    var player, videoPlayer, content, exp, initialVideo;
	var NeonTrackerURL = "http://localhost:8888/track";
    return {
        onTemplateLoad: function (expID){                                           
            console.log( "template loaded " + new Date().getTime());                 
			NeonPlayerTracker.hookNeonTrackerToFlashPlayer(expID);
        },
                                                       
		hookNeonTrackerToFlashPlayer: function(expID) { 
			console.log("Hoooked Neon Tracker!");
			player = brightcove.api.getExperience(expID);
			if (player){
				NeonTrackerType = "html5";
			}else{
            	player = bcPlayer.getPlayer(expID);                                      
				videoPlayer = player.getModule(APIModules.VIDEO_PLAYER);                 
            	content = player.getModule(APIModules.CONTENT);                         
				exp     = player.getModule(APIModules.EXPERIENCE); 
            	videoPlayer.addEventListener(BCMediaEvent.BEGIN, NeonPlayerTracker.onMediaBegin);
				exp.addEventListener(BCExperienceEvent.CONTENT_LOAD, NeonPlayerTracker.trackLoadedImageUrls);
			}
			//Test //setTimeout( function() { location.reload(); } , 5000);
		 },                             
 
        onTemplateReady: function (evt) {                                         
            console.log("template ready " + new Date().getTime());
			if (evt.target.experience) {
				APIModules = brightcove.api.modules.APIModules;
				videoPlayer = player.getModule(APIModules.VIDEO_PLAYER);
				content = player.getModule(APIModules.CONTENT);
				videoPlayer.addEventListener(brightcove.api.events.MediaEvent.BEGIN, NeonPlayerTracker.smartPlayerMediaBegin);
				videoPlayer.getCurrentVideo( function(videoDTO) { initialVideo = videoDTO;});
				console.log("smartplayer api");
			}else{
				initialVideo = videoPlayer.getCurrentVideo();
				NeonPlayerTracker.trackLoadedImageUrls();
			}
		},
		
		trackLoadedImageUrls: function(){
            var imageUrls = new Array();
            var stillUrls = new Array();                                            
			var mediaCollection = content.getAllMediaCollections();
            if (mediaCollection.length >0 && mediaCollection[0].mediaCount > 0){
            	for(var i = 0; i < mediaCollection[0].mediaCount; i++) {
                		imageUrls[i] = content.getMedia(mediaCollection[0].mediaIds[i]) ["thumbnailURL"].split('?')[0]; 
                		//stillUrls[i] = content.getMedia(mediaCollection[0].mediaIds[i]) ["videoStillURL"].split('?')[0];
            	}
			}
			//console.log(imageUrls);
			action = "load";
			params = "a=" + action + "&id="+ reqGuid + "&imgs=" + encodeURIComponent(JSON.stringify(imageUrls)) + "&cvid="+initialVideo.id;
			if (PageLoadIDSeen == null){
				PageLoadIDSeen = reqGuid;
				NeonDataSender.sendRequest(NeonTrackerURL,params);			           
			}
        },
                                                                                     
        onMediaBegin: function (evt) {
            console.log( "Media Begin ! " + new Date().getTime());            
		  	var vid    = evt.media.id;	
			var action = "click";
			var imgSrc = evt.media.thumbnailURL.split("?")[0]; //clean up
			//stillSrc = evt.media.videoStillURL;
			params = "a=" + action + "&id="+ reqGuid + "&img=" + encodeURIComponent(imgSrc);
			if(MediaPlayPageIDSeen == null){
				MediaPlayPageIDSeen = reqGuid;
				NeonDataSender.sendRequest(NeonTrackerURL,params);			         
			}  
			
        },

		smartPlayerMediaBegin: function (evt){
			videoPlayer.getCurrentVideo( function(videoDTO) {
				var imageUrls = new Array();
				imageUrls[0] = videoDTO.thumbnailURL.split("?")[0];
				action = "load";
				params = "a=" + action + "&id="+ reqGuid + "&imgs=" + encodeURIComponent(JSON.stringify(imageUrls)) + "&cvid="+initialVideo.id;
				NeonDataSender.sendRequest(NeonTrackerURL,params);

				action = "click";
				imgSrc = videoDTO.thumbnailURL.split("?")[0]; 
				params = "a=" + action + "&id="+ reqGuid + "&img=" + encodeURIComponent(imgSrc);
				NeonDataSender.sendRequest(NeonTrackerURL,params);
			});
		},

		setTestMode: function(){
			NeonTrackerURL = "http://localhost:8888/test"
			NeonDataSender.setTestMode()
		},
	
		testJsonCallback: function(jsonData){
			action = jsonData["a"];
			alert( "player " + action + " works");
		}	

    }
})();

//Test
NeonPlayerTracker.setTestMode()
