// Modify this to the VideoID of the video to be tested
	var img_class_to_test = "2474836017001";

	// Google Analytics.js
	
	(function(i,s,o,g,r,a,m){i['GoogleAnalyticsObject']=r;i[r]=i[r]||function(){
	 (i[r].q=i[r].q||[]).push(arguments)},i[r].l=1*new Date();a=s.createElement(o),
 	m=s.getElementsByTagName(o)[0];a.async=1;a.src=g;m.parentNode.insertBefore(a,m)
 	})(window,document,'script','//www.google-analytics.com/analytics.js','__gaTrackerNeon');

	__gaTrackerNeon('create', 'UA-40268565-1');
	
	//track the page view
	function neontrackPageView(video_id){
		__gaTrackerNeon('send', 'pageview',{
 		 	'page': '/neon-ppg-test-' + video_id ,
  		 	'title': 'Neon PPG ABTEST'
		});	
	}

	var select_random = null
	var neon_thumbnails_selected = false

	var readtokenForNeon = "hLGCV_uw2wWjyVxq6wgMMPHhLf3RjQbjeBWFnRgfxBFGsCaSAPYepg.." // PPG Token
	
	function neonresponse(jsonData) {
		//console.log("js " + JSON.stringify(jsonData))

		if(neon_thumbnails_selected == true)
			return;

		// Service call error
		if (jsonData == null || typeof jsonData === "undefined"){
			return;
		}
 
		var nthumbnails = 4
		if (select_random == null){
			select_random = Math.random()	
		}
		var randomNumber = Math.floor(select_random * nthumbnails)
		
		var source = null	
		try { 

			// designer thumbnail	
			if(randomNumber == 0){
				source = jsonData["videoStillURL"];
			}
			// mid point thumbnail
			else if(randomNumber == 1){
				source = jsonData["customFields"]["neonb"];
			}
			// filtered thumbnail 
			else if(randomNumber == 2){
				source = jsonData["customFields"]["neonc"];
			}
			// neon thumbnail
			else if(randomNumber == 3){
				source = jsonData["customFields"]["neona"];
			}
			if (source != null) {
				video_id = jsonData["id"]

				//track load event 
				try{
					__gaTrackerNeon('send','event', video_id, 'load', source);
				}catch(err){}
				
				// Set the source for the image class under test
				$("img."+img_class_to_test).attr("src", source)

				neon_thumbnails_selected = true
				

			}
		}
		catch(err) {
			return;
		}	
		
	}

	$(document).ready(function () {                                              
 
		var tracked_image_element = 0 

		// Populate the images
	
		var req = "http://api.brightcove.com/services/library?"
		req += "command=find_video_by_id&token=" + encodeURIComponent(readtokenForNeon);
		req += "&video_fields=customFields,id,videoStillURL"
		req += "&callback=neonresponse";
		req += "&video_id="+ img_class_to_test;

		// Create a new request object
		bObj = new JSONscriptRequest(req); 
		
		// Build the dynamic script tag
		bObj.buildScriptTag(); 
		
		// Add the script tag to the page
		bObj.addScriptTag();
        
	    $("img." + img_class_to_test).click(function () {
		
			tracked_image_element = this
			category = img_class_to_test	
			action = "click"
			label = $(this).attr('src')
			try {
				__gaTrackerNeon('send','event',category, action, label);
			} catch(err){}

	     }); 
  	});
