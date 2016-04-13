'use strict';

import videojs from 'video.js';
import reqwest from 'reqwest';

// Request basic events from the html5 component
//const VIDEO_EVENTS = videojs.getComponent('Html5').Events;

// Tracking defaults for the plugin
const defaults = {
    'trackUrl': 'http://tracker.neon-images.com/v2/track/',
    // Default events to remote log to Neon
    'trackEvents': [
        'image_load',
        'image_view',
        'image_click',
        'play',
        'ad_play',
        'video_play_percent',
    ]
};

// Mapping of Brightcove event type to Neon Api endpoint
const constants = {
    eventCodeMap: {
        'image_load': 'il',
        'image_view': 'iv',
        'image_click': 'ic',
        'play': 'vp',
        'ad_play': 'ap',
        'video_play_percent': 'vpp',
    }
};

// Unimplemented data fields in the Neon payload
const dummyParams = {
    'pageid': 'ajsidfh823u9',
    'tai': '56139847',
    'ttype': 'BRIGHTCOVE',
    'page': 'http%3A%2F%2Fwww.neon-lab.com%2Fproduct',
    'rts': 1406569946,
    'vid': 'alskdjf987',
    'pcount': 1
}


/**
 * @function onPlayerReady
 * @param    {Player} playegg
 * @param    {Object} [options={}]
 */
const onPlayerReady = (player, options) => {
    console.log('onPlayerReady');
    player.neon.options = options || {};
    player.on('image_load', trackImageLoad);
//    player.on('image_view', trackImageView);
//    player.on('image_click', trackImageClick);
    player.on('play', trackPlay);
//    player.on('ad_play', trackAdPlay);
//    player.on('video_play_percent', trackVideoPlayPercent);
};

/**
 * Defer setup to video player's ready event.
 *
 * @param    {Object} [options={}]
 */
const neonTracker = function(options) {
  this.ready(() => {
    onPlayerReady(this, videojs.mergeOptions(defaults, options));
  });
};

const trackPlay = (playerEvent) => {
    _commonTrack(playerEvent);
};

const trackImageLoad = (playerEvent) => {
    _commonTrack(playerEvent);
}

const trackImageView = (playerEvent) => {
    _commonTrack(playerEvent);
}

const trackImageClick = (playerEvent) => {
    _commonTrack(playerEvent);
}

const trackAdPlay = (playerEvent) => {
    _commonTrack(playerEvent);
}

const trackVideoPlayPercent = (playerEvent) => {
    _commonTrack(playerEvent);
}

const _commonTrack = (playerEvent, extra) => {
    console.log('player emit ' + playerEvent.type);
    extra = extra || {};
    if(player.neon.options.trackEvents.indexOf(playerEvent.type) >= 0) {
        remoteLogEvent(playerEvent.type, extra);
    }
}

const remoteLogEvent = (eventType, extra) => {

    let action = constants['eventCodeMap'][eventType];
    let data = videojs.mergeOptions(dummyParams, {'a': action});

    /*
     Resolve the CORS problem; implement the rest of tracked data
    let req = reqwest({
        url: player.neon.options['trackUrl'],
        method: 'get',
        crossDomain: true,
        data: data,
        success: function(response) {
            console.log(response);
        }
    });
    /**/
};


// Register the plugin with video.js.
videojs.plugin('neon', neonTracker);

// Include the version number.
neonTracker.VERSION = '__VERSION__';

export default neonTracker;
