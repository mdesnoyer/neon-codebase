'use strict';

import videojs from 'video.js';
import reqwest from 'reqwest';

// Request basic events from the html5 component
//const VIDEO_EVENTS = videojs.getComponent('Html5').Events;

// Consider throttling

// Tracking defaults for the plugin
const defaults = {

    // Default Neon api endpoint
    'trackUrl': 'http://tracker.neon-images.com/v2/track/',

    // Default events to remote log to Neon
    'trackEvents': [
        'image_load',
        'image_view',
        'image_click',
        'play',
        'ad_play',
        'timeupdate',
    ],

    // Interval in percent to send video play percent
    'timeupdateInterval': 25
};

// Mapping of Brightcove event type to Neon Api endpoint
const constants = {
    eventCodeMap: {
        'image_load': 'il',
        'image_view': 'iv',
        'image_click': 'ic',
        'play': 'vp',
        'ad_play': 'ap',
        'timeupdate': 'vpp',
    }
};

// Unimplemented data fields in the Neon payload
const dummyParams = {
    // Neon's publisher id
    'tai': '56139847',
    // Tracker yype
    'ttype': 'BRIGHTCOVE',
    // Url of event
    'page': 'http%3A%2F%2Fwww.neon-lab.com%2Fproduct',
    // Video identifier
    'vid': 'alskdjf987',
    // 1-based count of videos played on this pageload
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
    player.neon.pageId = _uuid();
    player.neon.percentsPlayed = [];
    player.on('image_load', trackImageLoad);
    player.on('image_view', trackImageView);
    player.on('image_click', trackImageClick);
    player.on('play', trackPlay);
    player.on('ad_play', trackAdPlay);
    player.on('timeupdate', trackVideoPlayPercent);

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

const _getPercentPlayed = () => {
    var currentTime, duration;
    currentTime = Math.round(player.currentTime());
    duration = Math.round(player.duration());
    return Math.round(currentTime / duration * 100);
}

const trackVideoPlayPercent = (playerEvent) => {
    var interval, percent
    interval = Math.min(100, player.neon.options.timeupdateInterval);
    percent = 0;

    for(;percent <= 100; percent += interval) {
        if(_getPercentPlayed() > percent) {
            if(player.neon.percentsPlayed.indexOf(percent) < 0) {
                player.neon.percentsPlayed.push(percent);
                _commonTrack(playerEvent, {'prcnt': percent});
            }
        }
    }
}

const _commonTrack = (playerEvent, extra) => {
    extra = extra || {};
    if(player.neon.options.trackEvents.indexOf(playerEvent.type) >= 0) {
        remoteLogEvent(playerEvent.type, extra);
    }
}

const remoteLogEvent = (eventType, extra) => {

    let action = constants['eventCodeMap'][eventType];
    let data = videojs.mergeOptions(dummyParams, {
        'a': action,
        'pageid': player.neon.pageId,
        'cts': (new Date()).getTime()
    });
    let url = player.neon.options['trackUrl'];
    console.log('remote', action, url, data)

    /*
     Resolve the CORS problem; implement the rest of tracked data
    let req = reqwest({
        url: player.neon.options['trackUrl'],
        method: 'get',
        crossDomain: true,
        data: data,
        success: function(response) { }
    });
    /**/
};

// Taken from the other js client code; need to know if this is fine
const _uuid = () => {
    function genRandomHexChars() {
        return Math.floor((1 + Math.random()) * 0x10000).toString(16).substring(1);
    }
    return genRandomHexChars() + genRandomHexChars() + genRandomHexChars() + genRandomHexChars();
}

// Register the plugin with video.js.
videojs.plugin('neon', neonTracker);

// Include the version number.
neonTracker.VERSION = '__VERSION__';

export default neonTracker;
