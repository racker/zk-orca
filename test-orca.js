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


test('get long', function(t) {
  var path = '/bar-' + randomstring.generate(8) ,
      cxn;

  cxn = zkorca.getCxn(defaultOptions());

  async.auto({
    get: function(callback) {
      cxn.get(path, function(err, data) {
        t.ok(err, 'Cannot get a value before it has been instantiated');
        callback();
      });
    },

    set: ['get', function(callback) {
      cxn.incrementAndGet(path, 'test-txn-id', function(err, value) {
        t.error(err, 'Should not error when incrementing a long');
        t.equal(value, 1, 'This value was just created and incremented, so should be 1');
        callback();
      });
    }],

    getAgain: ['set', function(callback) {
      cxn.get(path, function(err, data) {
        t.error(err, 'Should not error when getting the instantiated long');
        t.equal(data, 1, 'Value should still be 1');
        callback();
      });
    }]
  }, function(err) {
    t.error(err);
    t.end();
  });
});


test('get long initialize value zero', function(t) {
  var path = '/bar-' + randomstring.generate(8) ,
      cxn;

  cxn = zkorca.getCxn(defaultOptions());

  async.auto({
    get: function(callback) {
      cxn.get(path, 0, function(err, data) {
        t.error(err, 'We should not error because we provided an initial value');
        t.equal(data, 0, 'The value should be equal to what we initialized it to');
        callback();
      });
    },

    set: ['get', function(callback) {
      cxn.incrementAndGet(path, 'test-txn-id', function(err, value) {
        t.error(err, 'Should not error when incrementing a long');
        t.equal(value, 1, 'This value was just created and incremented, so should be 1');
        callback();
      });
    }],

    getAgain: ['set', function(callback) {
      cxn.get(path, function(err, data) {
        t.error(err, 'Should not error when getting the instantiated long');
        t.equal(data, 1, 'Value should still be 1');
        callback();
      });
    }],

    getAgain2: ['getAgain', function(callback) {
      cxn.get(path, 0, function(err, data) {
        t.error(err, 'Should not error when getting the instantiated long');
        t.equal(data, 1, 'Value should still be 1');
        callback();
      });
    }]
  }, function(err) {
    t.error(err);
    t.end();
  });
});


test('get long initialize value non-zero', function(t) {
  var path = '/bar-' + randomstring.generate(8) ,
    cxn;

  cxn = zkorca.getCxn(defaultOptions());

  async.auto({
    get: function(callback) {
      cxn.get(path, 1, function(err, data) {
        t.error(err, 'We should not error because we provided an initial value');
        t.ok(data, 'A value should be returned');
        t.equal(data, 1, 'The value should be equal to what we initialized it to');
        callback();
      });
    },

    set: ['get', function(callback) {
      cxn.incrementAndGet(path, 'test-txn-id', function(err, value) {
        t.error(err, 'Should not error when incrementing a long');
        t.equal(value, 2, 'This value was just created and incremented, so should be 1');
        callback();
      });
    }],

    getAgain: ['set', function(callback) {
      cxn.get(path, function(err, data) {
        t.error(err, 'Should not error when getting the instantiated long');
        t.equal(data, 2, 'Value should still be 1');
        callback();
      });
    }],

    getAgain2: ['getAgain', function(callback) {
      cxn.get(path, 1, function(err, data) {
        t.error(err, 'Should not error when getting the instantiated long');
        t.equal(data, 2, 'Value should still be 1');
        callback();
      });
    }]
  }, function(err) {
    t.error(err);
    t.end();
  });
});


test('increment long', function(t) {
  var path = '/bar-' + randomstring.generate(8) ,
      cxn;

  cxn = zkorca.getCxn(defaultOptions());
  cxn.incrementAndGet(path, 'test-txn-id', function(err, data) {
    t.error(err, 'Should be able to increment a long');
    t.ok(data, 'A value should be returned');
    t.equal(data, 1, 'The value should be equal to 1 since it was just created and incremented');
    t.end();
  });
});

test('increment long parallel', function(t) {
  var path = '/bar-' + randomstring.generate(8),
      cxn;

  cxn = zkorca.getCxn(defaultOptions());

  async.parallel([
    function increment1(callback) {
      cxn.incrementAndGet(path, 'test-txn-id', callback);
    },
    function increment2(callback) {
      cxn.incrementAndGet(path, 'test-txn-id', callback);
    },
    function increment3(callback) {
      cxn.incrementAndGet(path, 'test-txn-id', callback);
    }
  ], function(err, results) {
    var actual = results.sort(),
        expected = [1, 2, 3];

    t.error(err, 'Should not error while incrementing long 3 times');
    t.deepEqual(actual, expected, 'We should see each of 1, 2, 3 exactly once');
    t.end();
  });
});

test('increment long parallel sub-dir', function(t) {
  var path = '/bar/foo-' + randomstring.generate(8),
      cxn;

  cxn = zkorca.getCxn(defaultOptions());

  async.parallel([
    function increment1(callback) {
      cxn.incrementAndGet(path, 'test-txn-id', callback);
    },
    function increment2(callback) {
      cxn.incrementAndGet(path, 'test-txn-id', callback);
    },
    function increment3(callback) {
      cxn.incrementAndGet(path, 'test-txn-id', callback);
    }
  ], function(err, results) {
    var actual = results.sort(),
      expected = [1, 2, 3];

    t.error(err, 'Should not error while incrementing long 3 times');
    t.deepEqual(actual, expected, 'We should see each of 1, 2, 3 exactly once');
    t.end();
  });
});


test('increment by', function(t) {
  var path = '/bar-' + randomstring.generate(8),
      cxn;

  cxn = zkorca.getCxn(defaultOptions());
  cxn.incrementAndGetBy(path, 5, 'test-txn-id', function(err, value) {
    t.error(err, 'Should not error while incrementing a long by a value');
    t.ok(value);
    t.equal(value, 5, 'We incremented the value by 5, so it should be 5');
    t.end();
  });
});


test('decrement by', function(t) {
  var path = '/bar-' + randomstring.generate(8),
      cxn;

  cxn = zkorca.getCxn(defaultOptions());
  cxn.decrementAndGetBy(path, 5, 'test-txn-id', function(err, value) {
    t.error(err, 'Should not error while decrementing a long by a value');
    t.ok(value);
    t.equal(value, -5, 'We decremented the value by 5, so it should be -5');
    t.end();
  });
});

test('increment and decrement long', function(t) {
  var path = '/bar-' + randomstring.generate(8),
      cxn;

  cxn = zkorca.getCxn(defaultOptions());

  async.auto({
    increment1: function(callback) {
      cxn.incrementAndGet(path, 'test-txn-id', callback);
    },

    increment2: function(callback) {
      cxn.incrementAndGet(path, 'test-txn-id', callback);
    },

    increment3: function(callback) {
      cxn.incrementAndGet(path, 'test-txn-id', callback);
    },

    decrement1: ['increment1', function(callback) {
      cxn.decrementAndGet(path, 'test-txn-id', callback);
    }],

    decrement2: ['increment2', function(callback) {
      cxn.decrementAndGet(path, 'test-txn-id', callback);
    }],

    decrement3: ['increment3', function(callback) {
      cxn.decrementAndGet(path, 'test-txn-id', callback);
    }],

    get: ['decrement1', 'decrement2', 'decrement3', function(callback) {
      cxn.get(path, function(err, data) {
        t.error(err, 'Should not error while incrementing and decrementing long');
        t.equal(data, 0, 'Should be 0 after being incremented and decremented 3 times each');
        callback();
      });
    }]
  }, function(err) {
    t.error(err);
    t.end();
  });
});

test('bad path name', function(t) {
  var path = '/bar/',
      cxn;

  cxn = zkorca.getCxn(defaultOptions());
  cxn.incrementAndGet(path, 'test-txn-id', function(err) {
    t.ok(err, 'Should not be able to use a path that ends with a /');
    t.equal(err.message, 'Path name may not end in \'/\'');
    t.end();
  });

});

test('cleanup', function(t) {
  zkorca.shutdown(function() {
    t.end();
  });
});

