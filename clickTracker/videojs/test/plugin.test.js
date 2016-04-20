import document from 'global/document';

import QUnit from 'qunit';
import sinon from 'sinon';
import videojs from 'video.js';

import plugin from '../src/plugin';

const Player = videojs.getComponent('Player');

QUnit.test('the environment is sane', function(assert) {
  assert.strictEqual(typeof Array.isArray, 'function', 'es5 exists');
  assert.strictEqual(typeof sinon, 'object', 'sinon exists');
  assert.strictEqual(typeof videojs, 'function', 'videojs exists');
  assert.strictEqual(typeof plugin, 'function', 'plugin is a function');
});

QUnit.module('neon', {

  beforeEach() {

    // Mock the environment's timers because certain things - particularly
    // player readiness - are asynchronous in video.js 5. This MUST come
    // before any player is created; otherwise, timers could get created
    // with the actual timer methods!
    this.clock = sinon.useFakeTimers();

    this.fixture = document.getElementById('qunit-fixture');
    this.video = document.createElement('video');
    this.fixture.appendChild(this.video);
    this.player = videojs(this.video);
  },

  afterEach() {
    this.player.dispose();
    this.clock.restore();
  }
});

QUnit.test('registers itself with video.js', function(assert) {
  assert.expect(1);

  assert.strictEqual(
    Player.prototype.neon,
    plugin,
    'neon plugin was registered'
  );

  this.player.neon();

  // Tick the clock forward enough to trigger the player to be "ready".
  this.clock.tick(1);

  /*
  assert.ok(
    this.player.hasClass('vjs-neon-tracker'),
    'the plugin adds a class to the player'
  );
  */

});

/*
QUnit.test('util functions behave as expected', function(assert) {
  // This is a unit not a browser one, so it should go somewhere else
  let given = 'http://neonimage.com/here/123f34rfj/super%20space.jpg?andparams=true';
  let want = 'super%20space';
  assert.strictEqual(
      want,
      this.player.neon._getBasenameOf(given),
      'basename is filename without path or protocol or param'
  );
});
*/
