/* eslint indent:0 no-console:0*/
'use strict';

import videojs from 'video.js';
import reqwest from 'reqwest';
import printf from 'printf';
import _ from 'lodash';

// Package scope references to plugin and player
let neon;
let player;

// Runtime defaults for the plugin
const defaults = {

    // Default Neon api endpoint
    trackUrl: 'http://tracker.neon-images.com/v2/track',

    // Default events to remote log to Neon
    trackEvents: [
        'imageLoad',
        'imageView',
        'imageClick',
        'autoplay',
        'play',
        'adPlay',
        'timeUpdate'
    ],

    // Resolution (in percent points) to track video play percent
    timeUpdateInterval: 25,

    // Assume this is a Brightcove videojs context
    trackingType: 'BRIGHTCOVE',

    // Show console logging {true|false}
    showConsoleLogging: false
};

// Mapping of event type to Neon Api endpoint shorthand
const constants = {
    eventCodeMap: {
        imageLoad: 'il',
        imageView: 'iv',
        imageClick: 'ic',
        autoplay: 'vp',
        play: 'vp',
        adPlay: 'ap',
        timeUpdate: 'vvp'
    }
};

// Get the urlencoded location of the video player page
const _getPageUrl = () => {
    return encodeURIComponent(window.location.href);
};

// Get the urlencoded referrer of the video player page
const _getReferrer = () => {
    return encodeURIComponent(document.referrer);
};

// @TODO implement
const _getBasenameOf = (imageUrl) => {
    return imageUrl;
};

// Calculate the percentage of the video played
const _getPercentPlayed = () => {
    const currentTime = Math.round(player.currentTime());
    const duration = Math.round(player.duration());

    return Math.round(currentTime / duration * 100);
};

// Lifted from the Google Analytics plugin
const _adStateRegex = /(\s|^)vjs-ad-(playing|loading)(\s|$)/;
const _isInAdState = () => {
    return _adStateRegex.test(player.el().className);
};

// Specify valid keys for the api to guard against invalid request
const _trackerAllowedParams = [
    'a',
    'acount',
    'adelta',
    'aplay',
    'bn',
    'bns',
    'cts',
    'page',
    'pageid',
    'pcount',
    'playerId',
    'prcnt',
    'ref',
    'tai',
    'ttype',
    'vid'
];

// Run a ajax request for the log data
const _sendToTracker = (eventType, eventDetails) => {
    const action = constants.eventCodeMap[eventType];
    let pcount;

    if(neon.currentVid === undefined) {
        pcount = null;
    } else {
        pcount=  neon.playedVids.indexOf(neon.currentVid) + 1;
    }
    const data = _.pick(videojs.mergeOptions(

        // Page-level data
        neon.pageData,

        // Common event-level data
        {
            // Action identifier
            a: action,
            // Client's timestamp in localtime, in millis
            cts: (new Date()).getTime(),
            // 1-based index of video for videos in page
            pcount: pcount,
            ref: _getReferrer(),
            vid: neon.currentVid
        },

        // Event-type-specific data from eventDetails
        eventDetails

        // And filter unrecognized params
    ), _trackerAllowedParams);

    if (neon.options.showConsoleLogging) {
        console.log(printf('%s -> %s', eventType, action), data);
    }

    reqwest({
        url: neon.options.trackUrl,
        method: 'GET',
        crossOrigin: true,
        data
    });
};

// Check if event needs remote tracking in configured trackEvents
const _commonTrack = (neonEventType, eventDetails) => {
    eventDetails = eventDetails || {};
    if (neon.options.trackEvents.indexOf(neonEventType) >= 0) {
        _sendToTracker(neonEventType, eventDetails);
    }
};

const trackPlay = (playerEvent, eventDetails) => {
    if (_isInAdState()) {
        return;
    }

    neon.currentVid = _extractVideoId();
    if(neon.playedVids.indexOf(neon.currentVid) < 0) {
        neon.playedVids.push(neon.currentVid);
        neon.percentsPlayed[neon.currentVid] = {};
    }

    neon.hasVidPlayed = true;
    eventDetails = eventDetails || {};
    // Set no-autoplay flag
    eventDetails.aplay = false;
    // Set ad view flag
    eventDetails.adplay = neon.hasAdPlayed;
    _commonTrack('play', eventDetails);
};

const guessAutoplay = (e) => {
    // Autoplay emits no play event.
    // Thus if a player emits a timeUpdate without
    // a preceeding play, track this as an autoplay
    if (!neon.hasVidPlayed) {
        trackPlay({type: 'autoplay'}, {aplay: true});
    }
};

// Build basenames param string given list of map {url, width, height}
const _buildBnsParamFromList = (list) => {
    const values = [];

    list.forEach((dict) => {
        // Use space delimiter; the param will be urlencoded later
        values.push(printf('%s %d %d',
            _getBasenameOf(dict.url),
            dict.width,
            dict.height)
        );
    });

    return values.join(',');
};

const _getImagesFromEvent = (details) => {
    let url;
    let width;
    let height;
    const values = [];

    if (details.images !== undefined) {
        details.images.forEach((image) => {
            values.push({url, width, height});
        });
    } else if (player.poster() !== undefined) {
        url = player.poster();
        width = player.posterImage.width();
        height = player.posterImage.height();
        values.push({url, width, height});
    }
    return values;
};

const trackImageLoad = (playerEvent, eventDetails) => {
    eventDetails = eventDetails || {};
    const images = _getImagesFromEvent(eventDetails);

    if (images.length === 0) {
        console.log('Abort log player image load event: not enough info to continue');
        return;
    }
    eventDetails.bns = _buildBnsParamFromList(images);
    _commonTrack('imageLoad', eventDetails);
};

const trackImageView = (playerEvent, eventDetails) => {
    eventDetails = eventDetails || {};
    const images = _getImagesFromEvent(eventDetails);

    if (images.length === 0) {
        console.log('Abort log player image load event: not enough info to continue');
        return;
    }
    eventDetails.bns = _buildBnsParamFromList(images);
    _commonTrack('imageView', eventDetails);
};

const trackImageClick = (playerEvent, eventDetails) => {
    eventDetails = eventDetails || {};
    const images = _getImagesFromEvent(eventDetails);

    if (images.length === 0) {
        console.log('Abort log player image load event: not enough info to continue');
        return;
    }
    eventDetails.bn = _buildBnsParamFromList(images);
    _commonTrack('imageClick', eventDetails);
};

const trackAdPlay = (playerEvent, eventDetails) => {
    if (neon.hasAdPlayed) {
        return;
    }
    neon.hasAdPlayed = true;
    _commonTrack('adPlay', {aplay: false});
};

const trackVideoViewPercent = (playerEvent) => {

    // Measure the play progress by interval set in options
    const interval = Math.min(100, neon.options.timeUpdateInterval);
    const vid = neon.currentVid;
    // Begin at the first interval and not zero.
    let percent = interval;

    // Check for each step in [0..100], and keep
    // track of those we've already sent.
    for (; percent <= 100; percent += interval) {
        if (_getPercentPlayed() >= percent) {
            if (neon.percentsPlayed[vid].indexOf(percent) < 0) {
                neon.percentsPlayed[vid].push(percent);
                _commonTrack('timeUpdate', {prcnt: percent});
            }
        }
    }
};

const _uuid = () => {
    const randomString = (length, chars) => {
        let result = '';

        for (let i = length; i > 0; --i) {
            result += chars[Math.round(Math.random() * (chars.length - 1))];
        }
        return result;
    };

    // Taking from the other tracker Neon js implementations
    const _alphanum = '0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ';

    return randomString(16, _alphanum);
};

const _extractVideoId = () => {
    // Extract the video id
    const idKey = neon.options.publisherIdAttribute;
    let videoId = player.el().getAttribute(idKey);

    if (videoId === null) {
        throw new Error(
            'Fatal config error: player has no publisher id for key ' + idKey
        );
    }
    const regex = neon.options.publisherIdAttributeRegex;

    // Check the usefulness of this regex code
    if (regex !== undefined && regex !== null) {
        const matches = videoId.match(regex);

        if (matches !== null) {
            videoId = matches[0];
        } else {
            throw new Error(printf(
                'Fatal config error: video id %s does not match pattern %s',
                videoId,
                regex
            ));
        }
    }
    return videoId;
};

/**
 * @function onPlayerReady
 * @param    {Player} player
 * @param    {Object} [options={}]
 */
const onPlayerReady = (player_, options) => {

    player = player_;
    neon = player.neon;

    neon.options = options || {};
    neon.pageData = {
        // Fixed page idents
        pageid: _uuid(),
        page: _getPageUrl(),
        // Publisher id
        tai: options.publisherId,
        // Tracking type
        ttype: defaults.trackingType
    };

    // Store state of videos played
    neon.playedVids = [] 
    neon.percentsPlayed = {};
    neon.hasAdPlayed = false;
    neon.hasVidPlayed = false;

    player.on('image_load', trackImageLoad);
    player.on('image_view', trackImageView);
    player.on('image_click', trackImageClick);

    let posterUrl = player.poster();

    if (posterUrl !== undefined) {
        trackImageLoad({type: 'imageLoad'});
        trackImageView({type: 'imageView'});
        // Treat a play event as a image click for the poster
        player.one('play', trackImageClick);
    }

    player.on('play', trackPlay);
    player.on('ad-play', trackAdPlay);
    player.on('ima3-started', trackAdPlay);

    // Use timeupdate for detecting autoplay
    player.one('timeupdate', guessAutoplay);

    player.on('timeupdate', trackVideoViewPercent);
    player.on(['adstart', 'ads-ad-started', 'ima3-started'], trackAdPlay);
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

// Register the plugin with video.js
videojs.plugin('neon', neonTracker);

// Include the version number
neonTracker.VERSION = '__VERSION__';

export default neonTracker;
