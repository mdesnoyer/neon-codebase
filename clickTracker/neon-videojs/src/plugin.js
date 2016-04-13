'use strict';

import videojs from 'video.js';
import reqwest from 'reqwest';

// Tracking defaults for the plugin
const defaults = {
    'tracking_url': 'http://tracker.neon-images.com/v2/track/',
    // Default events to remote log to Neon
    'tracked_events': [
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


/**
 * @function onPlayerReady
 * @param    {Player} player
 * @param    {Object} [options={}]
 */
const onPlayerReady = (player, options) => {
    console.log('onPlayerReady', player, options);
    player.neon.options = options || {};
    player.on('image_load', trackImageLoad);
    player.on('image_view', trackImageView);
    player.on('image_click', trackImageClick);
    player.on('play', trackPlay);
    player.on('ad_play', trackAdPlay);
    player.on('video_play_percent', trackVideoPlayPercent);
};

const trackPlay = (player_event) => {
    console.log('video started');
    remoteLogEvent(player_event['type']);
};
const trackImageLoad = (player_event) => {
    console.log('player emit' + player_event.type);
}
const trackImageView = (player_event) => { console.log('player emit' + player_event.type); }
const trackImageClick = (player_event) => { console.log('player emit' + player_event.type); }
const trackAdPlay = (player_event) => { console.log('player emit' + player_event.type); }
const trackVideoPlayPercent = (player_event) => { console.log('player emit' + player_event.type); }

const remoteLogEvent = (event_type, extra) => {

    let action = constants['eventCodeMap'][event_type];
    let data = videojs.mergeOptions(dummyParams, {'a': action});

    let req = reqwest({
        url: player.neon.options['tracking_url'],
        method: 'get',
        data: data,
        success: function(response) {
            console.log(response);
        }
    });
};


/**
 * A video.js plugin.
 *
 * In the plugin function, the value of `this` is a video.js `Player`
 * instance. You cannot rely on the player being in a "ready" state here,
 * depending on how the plugin is invoked. This may or may not be important
 * to you; if not, remove the wait for "ready"!
 *
 * @function neonTracker
 * @param    {Object} [options={}]
 *           An object of options left to the plugin author to define.
 */
const neonTracker = function(options) {
  this.ready(() => {
    onPlayerReady(this, videojs.mergeOptions(defaults, options));
  });
};

const dummyParams = {
    'pageid': 'ajsidfh823u9',
    'tai': '56139847',
    'ttype': 'BRIGHTCOVE',
    'page': 'http%3A%2F%2Fwww.neon-lab.com%2Fproduct',
    'rts': 1406569946,
    'vid': 'alskdjf987',
    'pcount': 1
}

// Register the plugin with video.js.
videojs.plugin('neon', neonTracker);

// Include the version number.
neonTracker.VERSION = '__VERSION__';

export default neonTracker;
