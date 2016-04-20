// events.js
// List of usable videojs events associated to a particular namespace

// "Listen to all HTML5-defined events and trigger them on the player"
exports.player = [
    'abort',
    'canplay',
    'canplaythrough',
    'durationchange',
    'emptied',
    'ended',
    'error',
    'firstplay',
    'fullscreenchange',
    'loadeddata',
    'loadedmetadata',
    'loadedmetadata',
    'loadstart',
    'pause',
    'play',
    'playing',
    'posterchange'
    'progress',
    'ratechange',
    'seeked',
    'seeking',
    'stalled',
    'suspend',
    'texttrackchange',
    'timeupdate',
    'volumechange',
    'waiting',
];

exports.ads = [
    'ads-request',
    'ads-load',
    'ads-ad-started',
    'ads-ad-ended',
    'ads-pause',
    'ads-play',
    'ads-first-quartile',
    'ads-midpoint',
    'ads-third-quartile',
    'ads-click',
    'ads-volumechange',
    'ads-pod-started',
    'ads-pod-ended',
    'ads-allpods-completed',
    'ima3-ready',
    'contentupdate',
    'readyforpreroll',
    'contentplayback',
    'adtimeout',
    'contentended',
    'contentupdate',
    'contentplayback',
    'adsready',
    'adscanceled',
    'adstart',
    'adend',
    'ima3-started',
    'ima3-completed'
];
/**
  For reference, ad events
     // from docs.brightcove
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
     // From videojs-contrib-ads doc
     contentupdate (EVENT) — Fires when a new content video has been assigned to the player,
     so your integration can update its ad inventory. NOTE: This will NOT fire while your ad integration is playing a linear Ad.
     readyforpreroll (EVENT) — Fires when a content video is about to play for the first time,
     so your integration can indicate that it wants to play a preroll.
     contentplayback
     // From ads contrib source
     // events emitted by ad plugin
     adtimeout
     contentended
     contentupdate
     contentplayback
     // events emitted by third party ad implementors
     adsready
     adscanceled
     adstart
     adend
     // From ima3(google)
     ima3-started
     ima3-completed
     ima3-ready, ima3error, ima3-ad-error
/**/
