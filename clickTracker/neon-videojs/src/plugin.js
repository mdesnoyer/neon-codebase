'use strict';

import videojs from 'video.js';
import reqwest from 'reqwest';
import printf from 'printf';

// TODO Consider throttling
// TODO Implement for many-video-player pages
// TODO Understand ad play behavior
// TODO Implement the whole array of image events
/*
on *
+ ref (optional, string, `http%3A%2F%2Fwww.google.com`) ... The referral URL if available. URL Encoded Please
on vp, ap
+ adelta (optional, number, `30`) ... Time in milliseconds since the last click on page (or null if there wasn't one). Used to detect autoplay. Either this or aplay is required.
*/

// Consider: Use the GA plugin's ad detection regex
//  # get ad state of player
//  adStateRegex = /(\s|^)vjs-ad-(playing|loading)(\s|$)/
//  isInAdState = ( player ) =>
//    return adStateRegex.test( player.el().className )

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

    // Tracking type
    'trackingType': 'BRIGHTCOVE'
};
/**
 * For reference, ad events from docs.brightcove
     ads-request	Upon request ad data.
     ads-load	When ad data is available following an ad request.
     ads-ad-started	An ad has started playing.
     ads-ad-ended	An ad has finished playing.
     ads-pause	An ad is paused.
     ads-play	An ad is resumed from a pause.
     ads-first-quartile	The ad has played 25% of its total duration.
     ads-midpoint	The ad has played 50% of its total duration.
     ads-third-quartile	The ad has played 75% of its total duration.
     ads-click	A viewer clicked on the playing ad.
     ads-volumechange	The volume of the playing ad has been changed.
     ads-pod-started	The first ad in a linear ad pod (a sequenced group of ads) has started.
     ads-pod-ended	The last ad in a linear ad pod (a sequenced group of ads) has finished.
     ads-allpods-completed	All linear ads have finished playing.

     ima3-ready, ima3error, ima3-ad-error

     From videojs-contrib-ads
     contentupdate (EVENT) — Fires when a new content video has been assigned to the player, so your integration can update its ad inventory. NOTE: This will NOT fire while your ad integration is playing a linear Ad.
     readyforpreroll (EVENT) — Fires when a content video is about to play for the first time, so your integration can indicate that it wants to play a preroll.
     contentplayback
 */ 

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

};

const guessAutoplay = () => {
    // Autoplay emits no play event.
    // Thus if a player emits a timeupdate without
    // a preceeding play, track this as an autoplay
    if(!neon.hasVidPlayed) {
        neon.hasVidPlayed = true;
        trackPlay({'type': 'autoplay'}, {'aplay': true});
    }
}

const trackGenericStub = (e) => {
    console.log('emitted unimplemented ' + e.type);
};

const trackPlay = (playerEvent, extra) => {
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

const _commonTrack = (playerEvent, extra) => {
    extra = extra || {};
    if(neon.options.trackEvents.indexOf(playerEvent.type) >= 0) {
        remoteLogEvent(playerEvent.type, extra);
    }
}

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
            'pcount': 1
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

// Taking directly from the other Neon js implementations
// TODO review newer track code for this
const _uuid = () => {
    function genRandomHexChars() {
        return Math.floor((1 + Math.random()) * 0x10000).toString(16).substring(1);
    }
    return genRandomHexChars() + genRandomHexChars() + genRandomHexChars() + genRandomHexChars();
}

// Grab the urlencoded location of the video player page
const _getPageUrl = () => {
    return encodeURIComponent(window.location.href);
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

export default neonTracker;
