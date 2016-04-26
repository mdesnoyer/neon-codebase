# videojs-neon-tracker

Brightcove player event tracking for the Neon API.

Documentation is available http://docs.trackerneonlabcom.apiary.io/#reference/tracking-events/video-play

## Installation

```sh
npm install --save videojs-neon-tracker
```

## Usage

To include videojs-neon-tracker on your website or web application, use any of the following methods.

### `<script>` Tag

This is the simplest case. Get the script in whatever way you prefer and include the plugin _after_ you include [video.js][videojs], so that the `videojs` global is available.

```html
<script src="//path/to/video.min.js"></script>
<script src="//path/to/videojs-neon-tracker.min.js"></script>
<script>
    var player = videojs('my-video');
    player.neonTracker();
</script>
```

### Browserify

When using with Browserify, install videojs-neon-tracker via npm and `require` the plugin as you would any other module.

```js
var videojs = require('video.js');

// The actual plugin function is exported by this module, but it is also
// attached to the `Player.prototype`; so, there is no need to assign it
// to a variable.
require('videojs-neon-tracker');

var player = videojs('my-video');

player.neonTracker();
```

### RequireJS/AMD

When using with RequireJS (or another AMD library), get the script in whatever way you prefer and `require` the plugin as you normally would:

```js
require(['video.js', 'videojs-neon-tracker'], function(videojs) {
    var player = videojs('my-video');

    player.neonTracker();
});
```

## License

MIT. Copyright (c) Nate Kresge &lt;kresge@neon-lab.com&gt;


[videojs]: http://videojs.com/
