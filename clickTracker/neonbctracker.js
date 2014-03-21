var PageLoadIDSeen = null; 
var ImageTrackerLoadSeen = null; 
if (typeof NeonDataSender == "undefined"){
var NeonDataSender = (function() {
		var loadRequests = new Array();	
		var NeonTrackerURL = "http://tracker.neon-lab.com/track";
		//var NeonTrackerURL = "http://tracker-1719142472.us-east-1.elb.amazonaws.com/track";
		//var NeonTrackerURL = "http://localhost:8888/track";
		var images = new Array(); 
		var rParam = { 
			action: 0,
			ttype: 1,
			tai: 2,
			params: 3,
			imageUrls: 4	
		};

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
			createRequest: function(action, imgs, tai, params, ttype){
				// On page load combine the player and image tracker requests
				// build request
				var pageURL = (document.URL).split('?')[0]; // Ignore any get params	
				var ts = new Date().getTime();
				if (action == "load") {
					var request = new Array();
					request[rParam.action] = action;
					request[rParam.ttype] = ttype;
					request[rParam.tai] = tai;
					request[rParam.params] = params;
					request[rParam.imageUrls] = imgs;
					loadRequests.push(request);
				}else{
				// if click, then send immediately
				var req = NeonTrackerURL + "?" + "a=" + action + "&ts=" + ts + "&id=" + reqGuid + "&page=" + encodeURIComponent(pageURL) + "&ttype=" + ttype + "&tai=" + tai + params;
				NeonDataSender.sendRequest(req);
				}
				
			},
		
			checkRequestQueue: function(){
				// Iterate through the queue and combine the load requests
				var allImageUrls = new Array();
				var ts = new Date().getTime();
				var pageURL = (document.URL).split('?')[0]; // Ignore any get params
				var request = null;
				if (loadRequests.length == 0){
					return;
				}
				var ttype = "imagetracker";
				for (var i=0; i<loadRequests.length; i++){
					request = loadRequests[i];
					allImageUrls.push(request[rParam.imageUrls]);
					if (request[rParam.ttype] != ttype){
						ttype = request[rParam.ttype]
					}
				}
				loadRequests = new Array(); // clear the load requeust Array 
				image_urls = new Array();
				for (var i=0; i<allImageUrls.length; i++){
					imArr = allImageUrls[i];
					for (var j=0; j<imArr.length; j++){
						image_urls.push(imArr[j]);
					}
				}
				var req = NeonTrackerURL + "?" + "a=" + request[rParam.action] + "&ts=" + ts + "&id=" + reqGuid + "&page=" + encodeURIComponent(pageURL) + "&ttype=" + ttype + "&tai=" + request[rParam.tai] ;
				if (request[rParam.params] != null){ 
					req += request[rParam.params];
				}

				imgs = encodeURIComponent(JSON.stringify(image_urls));
				req += "&imgs=" + imgs;
				NeonDataSender.sendRequest(req);

			},

			sendRequest: function(req){
				try 
				{ 
					bObj = new JSONscriptRequest(req); 
					bObj.buildScriptTag(); 
					bObj.addScriptTag();  
				}
				catch(err){
					console.log(err)
				}
				
			},

			_NeonPageRequestUUID: function(){
				function genRandomHexChars() {
					return Math.floor((1 + Math.random()) * 0x10000).toString(16).substring(1); 
				}
				return genRandomHexChars() + genRandomHexChars() + genRandomHexChars() + genRandomHexChars();
			}
		}
	}());
}

var timerId = setInterval(NeonDataSender.checkRequestQueue, 1000); //1 second
// Flush the Queue on exit 
window.onbeforeunload = function (e) {
	NeonDataSender.checkRequestQueue();	
}

var reqGuid = NeonDataSender._NeonPageRequestUUID();
if (typeof NeonPlayerTracker == "undefined"){

	var NeonPlayerTracker = ( function () {
		var NeonTrackerType = "flashonlytracker";
		var player, videoPlayer, content, exp, initialVideo;
		var TrackerAccountId = null;
		return {
			onTemplateLoad: function (expID){                                         
				NeonPlayerTracker.hookNeonTrackerToFlashPlayer(expID);
			},

			setAccountId: function(aid){
				TrackerAccountId = aid;
			},

			hookNeonTrackerToFlashPlayer: function(expID) { 
				try { player = brightcove.api.getExperience(expID);} catch(err) {console.log(err);}
				if (player){
					NeonTrackerType = "html5";
				}else{
					player = bcPlayer.getPlayer(expID);                                     
					videoPlayer = player.getModule(APIModules.VIDEO_PLAYER);                 
					content = player.getModule(APIModules.CONTENT);                         
					exp     = player.getModule(APIModules.EXPERIENCE); 
					videoPlayer.addEventListener(BCMediaEvent.BEGIN, 
								NeonPlayerTracker.onMediaBegin);
					exp.addEventListener(BCExperienceEvent.CONTENT_LOAD, 
								NeonPlayerTracker.trackLoadedImageUrls);
					//advertising = player.getModule(APIModules.ADVERTISING);
					//advertising.addEventListener(BCAdvertisingEvent.AD_START, NeonPlayerTracker.onAdStart);
				}
			 },                             

	   		//onAdStart: function(evt){
			//	console.log(evt);
			//},	
			
			onTemplateReady: function (evt) {                                        
				if (evt.target.experience) {
					APIModules = brightcove.api.modules.APIModules;
					videoPlayer = player.getModule(APIModules.VIDEO_PLAYER);
					content = player.getModule(APIModules.CONTENT);
					videoPlayer.addEventListener(brightcove.api.events.MediaEvent.BEGIN, 
								NeonPlayerTracker.smartPlayerMediaBegin);
					videoPlayer.getCurrentVideo(
								function(videoDTO) {
									initialVideo = videoDTO;
								}
							);
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
				if (initialVideo){
					params = "&cvid=" + initialVideo.id;
				}else{
					params = "&cvid=null";
				}	
				if (PageLoadIDSeen == null){
					//avoid duplicate requests since template ready can get fired twice
					PageLoadIDSeen = reqGuid;
					NeonDataSender.createRequest(action, imageUrls, 
							TrackerAccountId, params, NeonTrackerType);			           
				}
			},
																						 
			onMediaBegin: function (evt) {
				var vid    = evt.media.id;	
				var action = "click";
				var imgSrc = evt.media.thumbnailURL.split("?")[0]; //clean up
				params = "&img=" + encodeURIComponent(imgSrc)
				NeonDataSender.createRequest(action, null, 
					TrackerAccountId, params, NeonTrackerType);			           
				
			},

			smartPlayerMediaBegin: function (evt){
				videoPlayer.getCurrentVideo( function(videoDTO) {
					var imageUrls = new Array();
					imageUrls[0] = videoDTO.thumbnailURL.split("?")[0];
					action = "load";
					params = "&cvid=" + initialVideo.id 
					NeonDataSender.createRequest(action, imageUrls, 
						TrackerAccountId, params, NeonTrackerType);

					action = "click";
					imgSrc = videoDTO.thumbnailURL.split("?")[0]; 
					params = "&img=" + encodeURIComponent(imgSrc)
					NeonDataSender.createRequest(action, null, 
							TrackerAccountId, params, NeonTrackerType);
				});
			} 

		}
	}());
}

if(typeof NeonImageTracker == "undefined"){
	var NeonImageTracker = ( function ()  {
		var initTracker = false;
		var NeonTrackerType = "imagetracker";
		var TrackerAccountId = null; 	

		return {
	
		getAccountId: function(){
			var scriptTags = document.getElementsByTagName("script");
			for (var i = 0; i<scriptTags.length; i++) {
				sTag = scriptTags[i];
				if(sTag.src.search("neonbctracker.js") >= 0){
					return sTag.id;
				} 		
			}	
		},

		setAccountId: function(aid){
				TrackerAccountId = aid;
				try {NeonPlayerTracker.setAccountId(aid);} catch(err){}
		},

		checkWindowReady: function(){
			if(document.readyState === "complete" || document.readyState === "interactive"){
					//Set Account ID 
					aid = NeonImageTracker.getAccountId();
				 	NeonImageTracker.setAccountId(aid);
					var action = "load";
					var imgTags = document.getElementsByTagName("img");
					if (!imgTags) {
						imgTags = jQuery(this).attr("img"); //use jquery
					}
					var imageUrls = new Array();
					for (var i = 0; i<imgTags.length; i++) {
						//imageUrls.push(imgTags[i].src.split('?')[0]);
						imageUrls.push(imgTags[i].src);
					}
					NeonDataSender.createRequest(action, imageUrls, 
							TrackerAccountId, null, NeonTrackerType);
					clearInterval(docReadyId);		
			}
		},

		trackerInit: function () {
			jQuery(document).ready(function () {

				if (initTracker) {
					return;
				}
				initTracker = true;
				// cease to use window.onload since it can be canceled or 
				// not compatible with certain browsers
				jQuery("img").click(function(e) {
					var action = "click";	
					var imgSrc = jQuery(this).attr('src');
					var coordinates = e.pageX  + "," + e.pageY;
					params = "&img=" + encodeURIComponent(imgSrc) + "&xy=" + coordinates 
					NeonDataSender.createRequest(action, null, 
							TrackerAccountId, params, NeonTrackerType);
				}); 
		});
		}
	   
	}
	}());

}

var docReadyId = setInterval(NeonImageTracker.checkWindowReady, 100); //100ms

// Only do anything if jQuery isn't defined
if (typeof jQuery == 'undefined') {

	if (typeof $ == 'function') {
		var thisPageUsingOtherJSLibrary = true;
	}

	//get script 		
	function getScript(url, success) {
	
		var script = document.createElement('script');
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
	getScript('http://ajax.googleapis.com/ajax/libs/jquery/1.7.2/jquery.min.js', function() {
	if (typeof jQuery=='undefined') {
			console.log("JQuery loading failed even after explicit load call");
		} else {
				// check if other conflicting library is present
				// try { $.noConflict(true); } catch(err) {}
				var $jq = jQuery.noConflict();
				//console.log("[debug] tracker jquery running in a sandbox");
				NeonImageTracker.trackerInit();
		}
	});
	
} else { 
	// jQuery was already loaded
	NeonImageTracker.trackerInit();
};

