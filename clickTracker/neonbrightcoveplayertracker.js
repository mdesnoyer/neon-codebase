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

	return{ 
		sendRequest: function(url, params){
			var pageURL = (document.URL).split('?')[0]; // Ignore any get params	
			var ts = new Date().getTime(); 
			var req = url + "?" + params + "&ts=" + ts + "&page=" + encodeURIComponent(pageURL) + "&ttype=" + NeonTrackerType;
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
var NeonPlayerTracker = ( function () {
    var player, videoPlayer, content, exp, initialVideo;
    var NeonTrackerURL = "http://tracker.neon-lab.com/track";
    var TrackerAccountId = "AccountIDNotSet";
	return {
        onTemplateLoad: function (expID){                                           
			NeonPlayerTracker.hookNeonTrackerToFlashPlayer(expID);
        },

		setAccountId: function(aid){
			NeonPlayerTracker.TrackerAccountId = aid;
		},

		hookNeonTrackerToFlashPlayer: function(expID) { 
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
		 },                             
 
        onTemplateReady: function (evt) {                                         
			if (evt.target.experience) {
				APIModules = brightcove.api.modules.APIModules;
				videoPlayer = player.getModule(APIModules.VIDEO_PLAYER);
				content = player.getModule(APIModules.CONTENT);
				videoPlayer.addEventListener(brightcove.api.events.MediaEvent.BEGIN, NeonPlayerTracker.smartPlayerMediaBegin);
				videoPlayer.getCurrentVideo( function(videoDTO) { initialVideo = videoDTO;});
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
            	}
			}
			action = "load";
			params = "a=" + action + "&id="+ reqGuid + "&imgs=" + encodeURIComponent(JSON.stringify(imageUrls)) + "&cvid="+initialVideo.id + "&tai=" + NeonPlayerTracker.TrackerAccountId;
			if (PageLoadIDSeen == null){
				PageLoadIDSeen = reqGuid;
				NeonDataSender.sendRequest(NeonTrackerURL,params);			           
			}
        },
                                                                                     
        onMediaBegin: function (evt) {
		  	var vid    = evt.media.id;	
			var action = "click";
			var imgSrc = evt.media.thumbnailURL.split("?")[0]; //clean up
			params = "a=" + action + "&id="+ reqGuid + "&img=" + encodeURIComponent(imgSrc) + "&tai=" + NeonPlayerTracker.TrackerAccountId;
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
				params = "a=" + action + "&id="+ reqGuid + "&imgs=" + encodeURIComponent(JSON.stringify(imageUrls)) + "&cvid="+initialVideo.id + "&tai=" + NeonPlayerTracker.TrackerAccountId;
				NeonDataSender.sendRequest(NeonTrackerURL,params);

				action = "click";
				imgSrc = videoDTO.thumbnailURL.split("?")[0]; 
				params = "a=" + action + "&id="+ reqGuid + "&img=" + encodeURIComponent(imgSrc) + "&tai=" + NeonPlayerTracker.TrackerAccountId;
				NeonDataSender.sendRequest(NeonTrackerURL,params);
			});
		} 

    }
}());
