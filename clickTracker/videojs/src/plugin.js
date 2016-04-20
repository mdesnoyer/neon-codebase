/* eslint indent:0 no-console:0*/
'use strict';

import videojs from 'video.js';
import reqwest from 'reqwest';
import printf from 'printf';
import _ from 'lodash';

// Package scope references to plugin and player
let neon;
let player;

// Runtime options defaults
const defaults = {

    // Publisher options defaults
    publisher: {

        // Set in html
        // id: <your neon id>

        // Default attribute of video tag that identifies the video content
        // e.g., <video id="videojs" data-video-id=<video_id>/> uses data-video-id
        videoIdAttribute: 'data-video-id',
        // A regex to further extract the identifier
        videoIdAttributeRegex: null
    },

    // Tracking options
    tracking: {
        // Default Neon api endpoint
        neonApiUrl: 'http://tracker.neon-images.com/v2/track',

        // Resolution (in percent points) to track video play percent
        timeUpdateInterval: 25,

        // Assume this is a Brightcove videojs context
        type: 'BRIGHTCOVE',

        // Default events to remote log to Neon
        events: [
            'imageLoad',
            'imageView',
            'imageClick',
            'autoplay',
            'play',
            'adPlay',
            'timeUpdate'
        ]
    },

    // Development options
    dev: {
        showConsoleLogging: false
    }
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

// Get the basename of a url
// e.g., "http://example.com/dir/thispart.jpg?withparams" => "thispart"
const _basenameRegex = /^.*\/|\.[^.]*$/g
const _getBasenameOf = (url) => {
    return url.replace(_basenameRegex, '');
};

// Calculate the percentage of the video played
const _getPercentPlayed = () => {
    const currentTime = Math.round(player.currentTime());
    const duration = Math.round(player.duration());

    return Math.round(currentTime / duration * 100);
};

// Determine if player is in ad-playing state by its element classes
// Regex from Google Analytics videojs plugin
// https://github.com/mickey/videojs-ga
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

    if (neon.currentVid === undefined) {
        pcount = null;
    } else {
        pcount = neon.playedVids.indexOf(neon.currentVid) + 1;
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
            pcount,
            ref: _getReferrer(),
            vid: neon.currentVid
        },

        // Event-type-specific data from eventDetails
        eventDetails

        // And filter unrecognized params
    ), _trackerAllowedParams);

    if (neon.options.dev.showConsoleLogging) {
        console.info(printf('%s -> %s', eventType, action), data);
    }

    reqwest({
        url: neon.options.tracking.neonApiUrl,
        method: 'GET',
        crossOrigin: true,
        data
    });
};

// Check if event needs remote tracking in configured trackEvents
const _commonTrack = (neonEventType, eventDetails) => {
    eventDetails = eventDetails || {};
    if (neon.options.tracking.events.indexOf(neonEventType) >= 0) {
        _sendToTracker(neonEventType, eventDetails);
    }
};

// Extract video id from page state based on config
const _extractVideoId = () => {
    // Extract the video id
    const idKey = neon.options.publisher.videoIdAttribute;
    let videoId = player.el().getAttribute(idKey);

    if (videoId === null) {
        throw new Error(
            'Fatal config error: player has no publisher id for key ' + idKey
        );
    }
    const regex = neon.options.publisher.videoIdAttributeRegex;

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

// Handle play event
const onPlay = (playerEvent, eventDetails) => {
    if (_isInAdState()) {
        return;
    }

    neon.currentVid = _extractVideoId();
    if (neon.playedVids.indexOf(neon.currentVid) < 0) {
        neon.playedVids.push(neon.currentVid);
        neon.percentsPlayed[neon.currentVid] = [];
    }

    eventDetails = eventDetails || {};
    // Set no-autoplay flag
    eventDetails.aplay = false;
    // Set ad view flag
    eventDetails.adplay = neon.hasAdPlayed;
    _commonTrack('play', eventDetails);
};

// Rough handle autoplay event
const guessAutoplay = (e) => {
    // Autoplay emits no play event.
    // Thus if a player emits a timeUpdate without
    // a preceeding play, track this as an autoplay
    if (neon.playedVids.length === 0) {
        onPlay({type: 'autoplay'}, {aplay: true});
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

// Thoughtfully get the images associated to the event
// return list of Object{url, width, height} or an empty list
const _getImagesForEvent = (playerEvent) => {
    let url;
    let width;
    let height;
    const values = [];

    if (playerEvent.images !== undefined) {
        playerEvent.images.forEach((image) => {
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

// Handle image load event
const onImageLoad = (playerEvent, eventDetails) => {
    eventDetails = eventDetails || {};
    const images = _getImagesForEvent(eventDetails);

    if (images.length === 0) {
        console.error(
            'Abort log player image load event: not enough info to continue');
        return;
    }
    eventDetails.bns = _buildBnsParamFromList(images);
    _commonTrack('imageLoad', eventDetails);
};

// Handle image view event
const onImageView = (playerEvent, eventDetails) => {
    eventDetails = eventDetails || {};
    const images = _getImagesForEvent(eventDetails);

    if (images.length === 0) {
        console.error(
            'Abort log player image load event: not enough info to continue');
        return;
    }
    eventDetails.bns = _buildBnsParamFromList(images);
    _commonTrack('imageView', eventDetails);
};

// Handle image click event
const onImageClick = (playerEvent, eventDetails) => {
    eventDetails = eventDetails || {};
    const images = _getImagesForEvent(eventDetails);

    if (images.length === 0) {
        console.error(
            'Abort log player image load event: not enough info to continue');
        return;
    }
    // Unlike the other image tracking event formats, the bn includes no dimension
    eventDetails.bn = _getBasenameOf(images[0].url);
    _commonTrack('imageClick', eventDetails);
};

// Handle ad play event
// Note there are various ad state models with different emitted events
// See events.js for more
const onAdPlay = (playerEvent, eventDetails) => {
    if (neon.hasAdPlayed) {
        return;
    }
    neon.hasAdPlayed = true;
    _commonTrack('adPlay', {aplay: false});
};

// Handle time update event; send once per interval per video
const onTimeUpdate = (playerEvent) => {

    // Measure the play progress by interval set in options
    const interval = Math.min(100, neon.options.tracking.timeUpdateInterval);
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

// Get a relatively unique random identifier
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

// Handle the poster change event
const onPosterChange = (playerEvent) => {

    // Naively treat a change as a load and a view
    onImageLoad(playerEvent);
    onImageView(playerEvent);

    // And first play event as a image click for the poster
    player.one('play', onImageClick);
};

// Handle the video.js ready event
const onPlayerReady = (player_, options) => {

    player = player_;
    neon = player.neon;

    neon.options = options;

    neon.pageData = {
        // Fixed page idents
        pageid: _uuid(),
        page: _getPageUrl(),
        // Publisher id
        tai: options.publisher.id,
        // Tracking type
        ttype: options.tracking.type
    };

    // Store state of videos played
    neon.playedVids = [];
    neon.percentsPlayed = {};
    neon.hasAdPlayed = false;

    // If the poster is set, track as though it had just changed
    if (player.poster() !== undefined) {
        onPosterChange();
    }

    // Associate events to their track handlers
    player.on('posterchange', onPosterChange);
    player.on('play', onPlay);
    player.on('ad-play', onAdPlay);
    player.on('timeupdate', onTimeUpdate);
    player.on(['adstart', 'ads-ad-started', 'ima3-started'], onAdPlay);

    // Use timeupdate for detecting autoplay
    player.one('timeupdate', guessAutoplay);
};

// Defer setup to video player's ready event.
const neonTracker = function(options) {
    this.ready(() => {
        onPlayerReady(this, videojs.mergeOptions(defaults, options));
    });
};

// Register the plugin with video.js
videojs.plugin('neon', neonTracker);

// Include the version number
neonTracker.VERSION = '0.0.1';

// Allow simple es6 import
export default neonTracker;
