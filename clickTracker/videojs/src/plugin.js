/* eslint indent:0 */
'use strict';

import videojs from 'video.js';
import reqwest from 'reqwest';
import printf from 'printf';

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

// Dummy dev params TODO
const dummyData = {
    // From the other trackers grab this from their api?
    // Video identifier
    vid: 'alskdjf987'
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

const _sendToConsole = (message) => {
    if (neon.options.showConsoleLogging) {
        console.info(message);
    }
};

// Run a ajax request for the log data
const _remoteLogEvent = (eventType, extra) => {
    let action = constants.eventCodeMap[eventType];
    let data = videojs.mergeOptions(

        // @TODO
        dummyData,

        neon.pageData,
        // Event-level data
        {
            // Action identifier
            a: action,
            // Client's timestamp in localtime, in millis
            cts: (new Date()).getTime(),
            // 1-based index of video for videos in page
            // @TODO
            pcount: 1,
            ref: _getReferrer()
        },
        // Event-type-specific extra data
        extra
    );

    let url = neon.options.trackUrl;

    _sendToConsole(printf('%s -> %s', eventType, action), url, data);

    // Implement the rest of tracked data
    reqwest({
        url: neon.options.trackUrl,
        method: 'GET',
        crossOrigin: true,
        data
    });
};

// Check if event needs remote tracking in configured trackEvents
const _commonTrack = (playerEvent, extra) => {
    extra = extra || {};
    if (neon.options.trackEvents.indexOf(playerEvent.type) >= 0) {
        _remoteLogEvent(playerEvent.type, extra);
    }
};

const trackImageLoad = (playerEvent, extra) => {
    let url;
    let width;
    let height;

    extra = extra || {};
    if (extra.image !== undefined) {
        url = extra.image.url;
        width = extra.image.width;
        height = extra.image.height;
    } else if (player.poster() !== undefined) {
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
};

const trackPlay = (playerEvent, extra) => {
    if (_isInAdState()) {
        return;
    }
    neon.hasVidPlayed = true;
    extra = extra || {aplay: false};
    extra.adplay = neon.hasAdPlayed;
    _commonTrack(playerEvent, extra);
};

const guessAutoplay = (e) => {
    // Autoplay emits no play event.
    // Thus if a player emits a timeUpdate without
    // a preceeding play, track this as an autoplay
    if (!neon.hasVidPlayed) {
        trackPlay({type: 'autoplay'}, {aplay: true});
    }
};

const trackImageView = (playerEvent) => {
    _commonTrack(playerEvent);
};

const trackImageClick = (playerEvent) => {
    _commonTrack(playerEvent);
};

const trackAdPlay = (playerEvent) => {
    if (neon.hasAdPlayed) {
        return;
    }
    neon.hasAdPlayed = true;
    _commonTrack(
        {type: 'adPlay'},
        {aplay: false}
    );
};

const trackVideoViewPercent = (playerEvent) => {

    // Measure the play progress by interval set in options
    const interval = Math.min(100, neon.options.timeUpdateInterval);
    // Begin at the first interval and not zero.
    let percent = interval;

    // Check for each step in [0..100], and keep
    // track of those we've already sent.
    for (; percent <= 100; percent += interval) {
        if (_getPercentPlayed() > percent) {
            if (neon.percentsPlayed.indexOf(percent) < 0) {
                neon.percentsPlayed.push(percent);
                _commonTrack(playerEvent, {prcnt: percent});
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
    neon.percentsPlayed = [];
    neon.hasAdPlayed = false;
    neon.hasVidPlayed = false;

    player.on('imageLoad', trackImageLoad);
    player.on('imageView', trackImageView);
    player.on('imageClick', trackImageClick);

    let posterUrl = player.poster();

    if (posterUrl !== undefined) {
        trackImageLoad({type: 'imageLoad'});
    }

    // Capture play events just for the first play.
    player.one('play', trackPlay);
    player.one('ad-play', trackAdPlay);
    player.one('ima3-started', trackAdPlay);
    player.one('timeUpdate', guessAutoplay);

    player.on('timeUpdate', trackVideoViewPercent);

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
