'use strict';

import videojs from 'video.js';
import reqwest from 'reqwest';
import printf from 'printf';
//import ads from './events';

// TODO consider throttling
// TODO Implement for many-video-player pages
// TODO Fix interaction between ad play and autoplay detection
// TODO Implement the whole array of image events
// TODO Implement adelta
// TODO implement pcount
// TODO review flash js bridge

// Reference to neon plugin after player initialized 
var neon;

// Tracking defaults for the plugin
const defaults = {

    // Default Neon api endpoint
    'trackUrl': 'http://tracker.neon-images.com/v2/track',

    // Default events to remote log to Neon
    'trackEvents': [
        'image_load',
        'image_view',
        'image_click',
        'autoplay',
        'play',
        'ad_play',
        'timeupdate',
    ],

    // Interval in percent to send video play percent
    'timeupdateInterval': 25,

    // Since the plugin is for Brightcove content
    'trackingType': 'BRIGHTCOVE'
};

// Mapping of Brightcove event type to Neon Api endpoint
const constants = {
    eventCodeMap: {
        'image_load': 'il',
        'image_view': 'iv',
        'image_click': 'ic',
        'autoplay': 'vp',
        'play': 'vp',
        'ad_play': 'ap',
        'timeupdate': 'vvp',
    }
};

// Dummy dev params TODO
const dummyData = {
    // Video identifier
    'vid': 'alskdjf987'
}

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

// Register the plugin with video.js
videojs.plugin('neon', neonTracker);

// Include the version number
neonTracker.VERSION = '__VERSION__';

/**
 * @function onPlayerReady
 * @param    {Player} player
 * @param    {Object} [options={}]
 */
const onPlayerReady = (player, options) => {

    neon = player.neon;

    neon.options = options || {};
    neon.pageData = {
        // Fixed page idents
        'pageid': _uuid(),
        'page': _getPageUrl(),
        // Publisher id
        'tai': options.publisherId,
        // Tracking type
        'ttype': defaults.trackingType
    };
    neon.percentsPlayed = [];
    neon.hasAdPlayed = false;
    neon.hasVidPlayed = false;

    player.on('image_load', trackImageLoad);
    player.on('image_view', trackImageView);
    player.on('image_click', trackImageClick);

    let posterUrl = player.poster();
    if(posterUrl !== undefined) {
        trackImageLoad({'type': 'image_load'});
    }

    // Only watch these for their first
    player.one('play', trackPlay);
    player.one('ad-play', trackAdPlay);
    player.one('ima3-started', trackAdPlay);
    player.one('timeupdate', guessAutoplay);

    player.on('timeupdate', trackVideoViewPercent);

    player.on(['adstart', 'ads-ad-started', 'ima3-started'], trackAdPlay);
};

const guessAutoplay = (e) => {
    // Autoplay emits no play event.
    // Thus if a player emits a timeupdate without
    // a preceeding play, track this as an autoplay
    if(!neon.hasVidPlayed) {
        trackPlay({'type': 'autoplay'}, {'aplay': true});
    }
}

const trackGenericStub = (e) => {
    console.log('emitted unimplemented ' + e.type);
};

const trackPlay = (playerEvent, extra) => {
    if(_isInAdState()) {
        return;
    }
    neon.hasVidPlayed = true;
    extra = extra || {'aplay': false};
    extra.adplay = neon.hasAdPlayed;
    _commonTrack(playerEvent, extra);
};

const trackImageLoad = (playerEvent, extra) => {
    let url, width, height;
    extra = extra || {};
    if(extra.image != undefined) {
        url = image.url;
        width = image.width;
        height = image.height;
    } else if(player.poster() != undefined) {
        url = player.poster();
        width = player.posterImage.width();
        height = player.posterImage.height();
    } else {
        // Can't effectively track without more info
        return;
    } 
    extra.bns = printf('%s %d %d',
        _getBasenameOf(url), width, height);
    _commonTrack(playerEvent, extra);
}

const trackImageView = (playerEvent) => {
    _commonTrack(playerEvent);
}

const trackImageClick = (playerEvent) => {
    _commonTrack(playerEvent);
}

const trackAdPlay = (playerEvent) => {
    if(neon.hasAdPlayed) {
        return;
    }
    neon.hasAdPlayed = true;
    _commonTrack(
        {'type': 'ad_play'},
        {'aplay': false}
    );
}
/*
+ pcount (required, number, `1`) ... 1-based counter for which video play this is on the page load. For example, if this is the 2nd video played in the given page load, the value should be 2.
+ aplay (optional, boolean, `false`) ... True if the video autoplayed. Either this or adelta is required.
/**/

const trackVideoViewPercent = (playerEvent) => {
    var interval, percent
    interval = Math.min(100, neon.options.timeupdateInterval);
    // Begin at the first interval and not zero.
    percent = interval;
    // Check for each step in [0..100], and keep
    // track of those we've already sent.
    for(; percent <= 100; percent += interval) {
        if(_getPercentPlayed() > percent) {
            if(neon.percentsPlayed.indexOf(percent) < 0) {
                neon.percentsPlayed.push(percent);
                _commonTrack(playerEvent, {'prcnt': percent});
            }
        }
    }
}

// Check if event needs remote tracking in configured trackEvents
const _commonTrack = (playerEvent, extra) => {
    extra = extra || {};
    if(neon.options.trackEvents.indexOf(playerEvent.type) >= 0) {
        remoteLogEvent(playerEvent.type, extra);
    }
}

// Run a ajax request for the log data
const remoteLogEvent = (eventType, extra) => {
    let action = constants['eventCodeMap'][eventType];
    let data = videojs.mergeOptions(

        // @TODO
        dummyData,

        neon.pageData,
        // Event-level data
        {
            // Action identifier
            'a': action,
            // Client's timestamp in localtime, in millis
            'cts': (new Date()).getTime(),
            // 1-based index of video for videos in page
            // @TODO
            'pcount': 1,
            'ref': _getReferrer()
        },
        // Event-type-specific extra data
        extra
    );

    let url = neon.options['trackUrl'];
    console.log(printf('%s -> %s', eventType, action), url, data)

    // Implement the rest of tracked data
    reqwest({
        url: neon.options['trackUrl'],
        method: 'GET',
        crossOrigin: true,
        data: data
    });
};

// Taking from the other tracker Neon js implementations
const _uuid = () => {
    function randomString(length, chars) {
        var result = '';
        for (var i = length; i > 0; --i) {
            result += chars[Math.round(Math.random() * (chars.length - 1))];
        }
        return result;
    }
    return randomString(16, '0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ');
};

// Get the urlencoded location of the video player page
const _getPageUrl = () => {
    return encodeURIComponent(window.location.href);
}

// Get the urlencoded referrer of the video player page
const _getReferrer = () => {
    return encodeURIComponent(document.referrer);
}
// @TODO implement
const _getBasenameOf = (imageUrl) => {
    return imageUrl;
}

// Calculate the percentage of the video played
const _getPercentPlayed = () => {
    var currentTime, duration;
    currentTime = Math.round(player.currentTime());
    duration = Math.round(player.duration());
    return Math.round(currentTime / duration * 100);
}

// Lifted from GA plugin
const adStateRegex = /(\s|^)vjs-ad-(playing|loading)(\s|$)/;
const _isInAdState = () => {
    return adStateRegex.test(player.el().className);
}


export default neonTracker;
