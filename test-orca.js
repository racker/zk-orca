/*
Copyright 2013 Rackspace Hosting, Inc

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

var test = require('tape');
var async = require('async');
var logmagic = require('logmagic');
var longjohn = require('longjohn');
var randomstring = require('randomstring');
var zkorca = require('./orca');
var _ = require('underscore');

if (process.env.TRACE) {
  logmagic.route("__root__", logmagic.TRACE1, "console");
}

var URLS = ['127.0.0.1:2181'];
var BAD_URLS = ['127.0.0.1:666'];
var DEFAULT_NAME = 'nameA';

if (process.env.ZOOKEEPER_PORT_2181_TCP_ADDR && process.env.ZOOKEEPER_PORT_2181_TCP_PORT) {
  URLS = [process.env.ZOOKEEPER_PORT_2181_TCP_ADDR + ":" + process.env.ZOOKEEPER_PORT_2181_TCP_PORT];
} else if (process.env.ZK) {
  URLS = [process.env.ZK];
}

function defaultOptions() {
  return {
    urls: URLS,
    name: DEFAULT_NAME
  }
}

//test('test monitor zone change', function(t) {
//  var cxn = zkorca.getCxn(defaultOptions());
//  cxn.monitor('acOne', 'testZone', function(err) {
//    t.ifError(err);
//  });
//  cxn.on('error', function(err) {
//    t.ifError(err);
//  });
//  cxn.on('zone:acOne:testZone', function(event) {
//    console.log(event);
//    t.end();
//  });
//  _.delay(function() {
//    cxn.addNode('acOne', 'testZone', 'agentId1', 'guid', function(err) {
//      t.ifError(err);
//    });
//  }, 100);
//});

function genDoubleBarrierKey(prefix) {
  prefix = prefix || 'double-barrier-';
  return '/' + prefix + randomstring.generate(8);
}

test('double barrier (2 nodes, create sync)', function(t) {
  var cxn = zkorca.getCxn(defaultOptions()),
      barrierEntryCount = 2,
      count = barrierEntryCount;

  function done(err) {
    t.ifError(err);
    count--;
    if (count == 0) {
      t.end();
    }
  }

  _.times(barrierEntryCount, cxn._doubleBarrierEnter.bind(cxn, genDoubleBarrierKey(), barrierEntryCount, -1, done));
});

test('double barrier (3 nodes, ordered)', function(t) {
  var barrierEntryCount = 3,
      count = barrierEntryCount,
      key = genDoubleBarrierKey();
  function done(err) {
    t.ifError(err);
    count--;
    if (count == 0) {
      t.end();
    }
  }
  setTimeout(function() {
    var cxn = zkorca.getCxn(defaultOptions());
    cxn._doubleBarrierEnter(key, barrierEntryCount, -1, done);
  }, 100);
  setTimeout(function() {
    var cxn = zkorca.getCxn(defaultOptions());
    cxn._doubleBarrierEnter(key, barrierEntryCount, -1, done);
  }, 200);
  setTimeout(function() {
    var cxn = zkorca.getCxn(defaultOptions());
    cxn._doubleBarrierEnter(key, barrierEntryCount, -1, done);
  }, 300);
});

test('double barrier (3 nodes, same time)', function(t) {
  var barrierEntryCount = 3,
      count = barrierEntryCount,
      key = genDoubleBarrierKey();
  function done(err) {
    t.ifError(err);
    count--;
    if (count == 0) {
      t.end();
    }
  }
  setTimeout(function() {
    var cxn = zkorca.getCxn(defaultOptions());
    cxn._doubleBarrierEnter(key, barrierEntryCount, -1, done);
  }, 0);
  setTimeout(function() {
    var cxn = zkorca.getCxn(defaultOptions());
    cxn._doubleBarrierEnter(key, barrierEntryCount, -1, done);
  }, 0);
  setTimeout(function() {
    var cxn = zkorca.getCxn(defaultOptions());
    cxn._doubleBarrierEnter(key, barrierEntryCount, -1, done);
  }, 0);
});

test('double barrier (3 nodes, random)', function(t) {
  var barrierEntryCount = 3,
      count = barrierEntryCount,
      key = genDoubleBarrierKey();
  function done(err) {
    t.ifError(err);
    count--;
    if (count == 0) {
      t.end();
    }
  }
  setTimeout(function() {
    var cxn = zkorca.getCxn(defaultOptions());
    cxn._doubleBarrierEnter(key, barrierEntryCount, -1, done);
  }, _.random(0, 100));
  setTimeout(function() {
    var cxn = zkorca.getCxn(defaultOptions());
    cxn._doubleBarrierEnter(key, barrierEntryCount, -1, done);
  }, _.random(0, 100));
  setTimeout(function() {
    var cxn = zkorca.getCxn(defaultOptions());
    cxn._doubleBarrierEnter(key, barrierEntryCount, -1, done);
  }, _.random(0, 100));
});

test('double barrier (random nodes [10-100], random)', function(t) {
  var barrierEntryCount = _.random(10, 100),
      count = barrierEntryCount,
      key = genDoubleBarrierKey();
  function done(err) {
    t.ifError(err);
    count--;
    if (count == 0) {
      t.end();
    }
  }
  _.times(barrierEntryCount, function() {
    setTimeout(function() {
      var cxn = zkorca.getCxn(defaultOptions());
      cxn._doubleBarrierEnter(key, barrierEntryCount, -1, done);
    }, _.random(0, 100));
  });
});

test('cleanup', function(t) {
  zkorca.shutdown(function() {
    t.end();
  });
});

