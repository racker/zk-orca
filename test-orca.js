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
var sprintf = require('sprintf').sprintf;
var uuid = require('uuid');
var zkorca = require('./orca');
var _ = require('underscore');

if (process.env.TRACE) {
  logmagic.route("__root__", logmagic.TRACE1, "console");
}

var URLS = ['127.0.0.1:2181'];
var BAD_URLS = ['127.0.0.1:6667'];
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

function genDoubleBarrierKey(prefix) {
  prefix = prefix || 'double-barrier-';
  return '/' + prefix + randomstring.generate(8);
}

test('test monitor zone change', function(t) {
  var cxn, acId, mzId, agentId, myPath;

  acId = 'acOne';
  mzId = 'testZone2';
  agentId = 'agentId1';

  function onError(err) {
    t.ifError(err, 'check for error');
  }

  function onZoneId() {
    async.auto({
      'getConnections': function(callback) {
         cxn.getConnections(acId, mzId, callback);
      },
      'validateConnections': ['getConnections', function(callback, results) {
        t.ok(results.getConnections[agentId].length == 1, 'check for one connection');
        _.delay(callback, 0);
      }],
      'isPrimary': ['validateConnections', function(callback, results) {
        cxn.isPrimary(acId, mzId, agentId, myPath, callback);
      }],
      'remove': ['isPrimary', function(callback) {
        cxn.removeNode(myPath, callback);
      }]
    }, function(err, results) {
      t.ok(results.isPrimary, 'should be primary');
      t.end();
    });
  }

  function onMonitor() {
    cxn.addNode(acId, mzId, agentId, uuid.v4(), function(err, _myPath) {
      t.ifError(err, 'check for error');
      myPath = _myPath;
    });
  }

  cxn = zkorca.getCxn(defaultOptions());
  cxn.monitor(acId, mzId);
  cxn.on('error', onError);
  cxn.on(zkorca.getZoneId(acId, mzId), onZoneId);
  cxn.once(zkorca.getMonitorId(acId, mzId), onMonitor);
});

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
  _.delay(function() {
    var cxn = zkorca.getCxn(defaultOptions());
    cxn._doubleBarrierEnter(key, barrierEntryCount, -1, done);
  }, 100);
  _.delay(function() {
    var cxn = zkorca.getCxn(defaultOptions());
    cxn._doubleBarrierEnter(key, barrierEntryCount, -1, done);
  }, 200);
  _.delay(function() {
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
  _.delay(function() {
    var cxn = zkorca.getCxn(defaultOptions());
    cxn._doubleBarrierEnter(key, barrierEntryCount, -1, done);
  }, 0);
  _.delay(function() {
    var cxn = zkorca.getCxn(defaultOptions());
    cxn._doubleBarrierEnter(key, barrierEntryCount, -1, done);
  }, 0);
  _.delay(function() {
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
  _.delay(function() {
    var cxn = zkorca.getCxn(defaultOptions());
    cxn._doubleBarrierEnter(key, barrierEntryCount, -1, done);
  }, _.random(0, 100));
  _.delay(function() {
    var cxn = zkorca.getCxn(defaultOptions());
    cxn._doubleBarrierEnter(key, barrierEntryCount, -1, done);
  }, _.random(0, 100));
  _.delay(function() {
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
    _.delay(function() {
      var cxn = zkorca.getCxn(defaultOptions());
      cxn._doubleBarrierEnter(key, barrierEntryCount, -1, done);
    }, _.random(0, 100));
  });
});

test('double barrier (timeout)', function(t) {
  var barrierEntryCount = 3,
      count = barrierEntryCount,
      key = genDoubleBarrierKey(),
      timeout = 100,
      cxn = zkorca.getCxn(defaultOptions());
  cxn._doubleBarrierEnter(key, barrierEntryCount, timeout, function(err) {
    t.ok(err instanceof zkorca.TimeoutException);
    t.end();
  });
});

test('double barrier leave', function(t) {
  var barrierEntryCount = 3,
      count = barrierEntryCount,
      key = genDoubleBarrierKey();
  function done(err) {
    t.ifError(err, 'make sure error is nil');
    count--;
    if (count == 0) {
      t.end();
    }
  }
  _.delay(function() {
    var cxn = zkorca.getCxn(defaultOptions());
    cxn._doubleBarrierEnter(key, barrierEntryCount, -1, function (err, path) {
      t.ifError(err, 'make sure error is nil');
      cxn._doubleBarrierLeave(path, done);
    });
  }, _.random(0, 100));
  _.delay(function() {
    var cxn = zkorca.getCxn(defaultOptions());
    cxn._doubleBarrierEnter(key, barrierEntryCount, -1, function (err, path) {
      t.ifError(err, 'make sure error is nil');
      cxn._doubleBarrierLeave(path, done);
    });
  }, _.random(0, 100));
  _.delay(function() {
    var cxn = zkorca.getCxn(defaultOptions());
    cxn._doubleBarrierEnter(key, barrierEntryCount, -1, function(err, path) {
      t.ifError(err, 'make sure error is nil');
      cxn._doubleBarrierLeave(path, done);
    });
  }, _.random(0, 20));
});

test('test monitor (add 3, remove 1)', function(t) {
  var cxn,
      acId = 'acOne',
      mzId = 'testZone2' + randomstring.generate(4),
      agentId = 'agentId1',
      paths = [];

  function onError(err) {
    t.ifError(err, 'check for error');
  }

  function onZoneChange() {
    async.auto({
      'getConnections': function(callback) {
         cxn.getConnections(acId, mzId, callback);
      },
      'validateConnections': ['getConnections', function(callback, results) {
        t.ok(results.getConnections[agentId].length > 0, 'check for connection');
        callback();
      }],
      'removeConnection': ['validateConnections', function(callback) {
        cxn.monitor(acId, mzId); // reregister
        cxn.once(zkorca.getZoneId(acId, mzId), function() {
          cxn.getConnections(acId, mzId, function(err, conns) {
            t.ifError(err);
            t.ok(conns[agentId].length > 0, 'check for two connection');
            t.end();
          });
        });
        _.delay(function() {
          cxn.removeNode(paths[0], callback);
        }, 1000);
      }]
    }, function(err) {
      t.ifError(err);
    });
  }

  function onMonitor() {
    function create(index, callback) {
      cxn.addNode(acId, mzId, agentId, uuid.v4(), function(err, path) {
        t.ifError(err, 'check for error');
        paths.push(path);
        callback();
      });
    }
    async.times(3, create);
  }

  cxn = zkorca.getCxn(defaultOptions());
  cxn.monitor(acId, mzId);
  cxn.on('error', onError);
  cxn.once(zkorca.getZoneId(acId, mzId), onZoneChange);
  cxn.once(zkorca.getMonitorId(acId, mzId), onMonitor);
});

test('cleanup', function(t) {
  zkorca.shutdown(function() {
    t.end();
  });
});

