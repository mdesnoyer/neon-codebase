# videojs-neon-tracker

Brightcove player event tracking for the Neon API.

Neon's API documentation is available http://docs.trackerneonlabcom.apiary.io/#reference/tracking-events/video-play. 

## Description

The purpose of the plugin is to send tracking events to the Neon event tracker. The new Brightcove player uses a library called video.js. The library allows for plugins to extend the base player. This tracker is built as one of these plugin. The events I need to capture are video plays, video progress, adplays, image loads, image views and image clicks. Our api is defined here http://docs.trackerneonlabcom.apiary.io/#

On Brightcove Cloud Studio CMS, you can choose js and json to be bundled into the video.js player instance so that the entire payload is loaded in one http request. The intended way to use the plugin here is to have it bundled in and then loaded as a <video> and <script> tag in your html. You can see an example of this in brightcove_published.html. The html files are all just examples; the production-use case is just to have the player on S3, have it bundled into the player js, and to be served from http://players.brightcove.net.

## Installation

```sh
npm install --save
```

## Building for distribution

```sh
npm build
```
Look in dist/ for the full and minified versions of the bundled plugin javascript.

## Examples

Run the dev web server.
```sh
npm start
```
Open http://localhost:9999/example/brightcove.html. You can configure the publisher id, video id, and plugin url to use your own. Look at development.html for an alternate example.

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

### Brightcloud Api

As an alternative to the above, you can bundle the plugin javascript into a Brightcove player via its CMS. You'll need to configure the plugin with json that includes your Neon account id.

## License

MIT. Copyright (c) Neon Lab, Inc.

## Contact
Nate Kresge &lt;kresge@neon-lab.com&gt;


[videojs]: http://videojs.com/
