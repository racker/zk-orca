/*
Copyright 2015 Rackspace Hosting, Inc

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

/**
 * @example

/:name/:accountKey/:zoneName/connections/agentId/connectionGuid

var zkorca = require('zk-orca');
var options = {
  urls: ['127.0.0.1:2181'],
  quorum: 3,
  change_trigger: 0
};
var cxn = zkorca.getCxn(options);
cxn.monitor('accountKey', 'zoneName')
cxn.on('zone:accountKey:zoneName', onZoneChange);
cxn.addNode('accountKey', 'zoneName', 'agentId', 'connection guid');
cxn.removeNode(path);

*/

var _ = require('underscore');
var async = require('async');
var events = require('events');
var log = require('logmagic').local('zk-ultralight');
var path = require('path');
var sprintf = require('sprintf').sprintf;
var uuid = require('uuid');
var util = require('util');
var zkUltra = require('zk-ultralight');
var zookeeper = require('node-zookeeper-client');

var DEFAULT_TIMEOUT = 16000;
var DELIMITER = ':';
var cxns = {}; // urls -> ZkCxn


function TimeoutException() {
  Error.call(this);
  this.message = 'timeout';
}
util.inherits(TimeoutException, Error);
exports.TimeoutException = TimeoutException;


/**
 *
 * @param opts
 * @returns {*}
 */
exports.getCxn = function getCxn(opts) {
  log.trace1('getCxn');
  var urls = opts.urls.join(',');
  if (!cxns[urls]) {
    cxns[urls] = new ZkOrca(opts);
  }
  return cxns[urls];
};


/**
 * Shutdown all ZK clients.
 * @param callback
 */
exports.shutdown = function shutdown(callback) {
  log.trace1('shutdown');
  var toClose = _.values(cxns);
  cxns = {};
  async.forEach(toClose, function(cxn, callback) {
    cxn.close(function closing(err) {
      if (err) {
        log.trace1('Error observed mid-shutdown', { error: err });
      }
      callback(); // suppress the error on shutdown
    });
  }, callback);
};


/**
 * Generate the zone ID for an account and zone.
 * @param {String} accountId the account id.
 * @param {String} zoneId the zone id.
 * @returns {String} the id for monitors.
 */
exports.getZoneId = function getZoneId(accountId, zoneId) {
  return sprintf('zone:%s:%s', accountId, zoneId);
};


/**
 * Generate the event ID for an account and zone.
 * @param {String} accountId the account id.
 * @param {String} zoneId the zone id.
 * @returns {String} the id for monitors.
 */
exports.getMonitorId = function getMonitorId(accountId, zoneId) {
  return sprintf('monitor:%s:%s', accountId, zoneId);
};


/**
 * Main Orcha Class.
 * @param opts
 * @constructor
 */
function ZkOrca(opts) {
  opts = opts || {};
  this._urls = opts.urls || [];
  this._timeout = opts.timeout || DEFAULT_TIMEOUT;
  this._name = opts.name || 'undefined_name';
  this._zku = zkUltra.getCxn(this._urls, this._timeout);
}
util.inherits(ZkOrca, events.EventEmitter);


/**
 * Monitor a subtree by accountKey and zoneName.
 * @param accountKey{String} the account key.
 * @param zoneName{String} the zone name.
 */
ZkOrca.prototype.monitor = function(accountKey, zoneName) {
  var self = this,
      path = sprintf('%s/connections', self._basePath(accountKey, zoneName));
  function onConnection(err) {
    if (err) {
      log.trace1('Error while waiting for connection (monitor)', { err: err });
      callback(err);
      return;
    }
    async.auto({
      'path': function(callback) {
        self._zku._zk.mkdirp(path, callback);
      },
      'watch': ['path', function(callback) {
        log.trace1('Starting watch', { accountKey: accountKey, zoneName: zoneName });
        function watch(event) {
          self.emit(exports.getZoneId(accountKey, zoneName), event);
        }
        self._zku._zk.getChildren(path, watch, function(err) {
          if (!err) {
            self.emit(exports.getMonitorId(accountKey, zoneName));
          }
          callback(err);
        });
      }]
    });
  }
  self._zku.onConnection(onConnection);
};


/**
 * Get a subtree connections.
 * @param accountKey{String} the account key.
 * @param zoneName{String} the zone name.
 */
ZkOrca.prototype.getConnections = function(accountKey, zoneName, callback) {
  var self = this,
      path = sprintf('%s/connections', self._basePath(accountKey, zoneName));
  function onConnection(err) {
    if (err) {
      log.trace1('Error while waiting for connection (monitor)', { err: err });
      callback(err);
      return;
    }
    self._zku._zk.getChildren(path, function(err, children) {
      if (err) {
        callback(err);
        return;
      }
      callback(null, _.groupBy(children, function(child) {
        return child.split(DELIMITER)[0];
      }));
    });
  }
  self._zku.onConnection(onConnection);
};


/**
 * Is a path a primary connection.
 * @param accountKey{String} the account key.
 * @param zoneName{String} the zone name.
 * @param agentId{String} the AgentID.
 * @param myPath{String} The path of the connection.
 * @param callback{Function} The callback (err, true/false);
 */
ZkOrca.prototype.isPrimary = function(accountKey, zoneName, agentId, myPath, callback) {
  var self = this;
  self.getConnections(accountKey, zoneName, function(err, conns) {
    var connTuple, sorted, isPrimary;
    if (err) {
      callback(err);
      return;
    }
    isPrimary = false;
    connTuple = path.basename(myPath).split(DELIMITER);
    if (conns[agentId] && conns[agentId].length > 0) {
      sorted = _.sortBy(conns[agentId], function(conn) {
        return conn.split(DELIMITER)[2];
      });
      isPrimary = sorted[0].lastIndexOf(connTuple[2]) > -1;
    }
    callback(null, isPrimary);
  });
};


ZkOrca.prototype._basePath = function(accountKey, zoneName) {
  return sprintf('/%s/%s/%s', this._name, accountKey, zoneName);
};


/**
 *
 * @param accountKey
 * @param zoneName
 * @param agentId
 * @param connectionGuid
 */
ZkOrca.prototype.addNode = function(accountKey, zoneName, agentId, connGuid, callback) {
  var self = this,
      connPath = sprintf('%s/connections/%s%s%s%s', self._basePath(accountKey, zoneName), agentId, DELIMITER, connGuid, DELIMITER);
  function onConnection(err) {
    if (err) {
      log.trace1('Error while waiting for connection (monitor)', { err: err });
      callback(err);
      return;
    }
    async.auto({
      'mkdir': function(callback) {
        self._zku._zk.mkdirp(path.dirname(connPath), callback);
      },
      'create': ['mkdir', function(callback) {
        self._zku._zk.create(connPath, null, zookeeper.CreateMode.EPHEMERAL_SEQUENTIAL, callback);
      }]
    }, function(err, results) {
      callback(err, results.create);
    });
  }
  self._zku.onConnection(onConnection);
};


ZkOrca.prototype.removeNode = function(path, callback) {
  var self = this;
  function onConnection(err) {
    if (err) {
      log.trace1('Error while waiting for connection (monitor)', { err: err });
      callback(err);
      return;
    }
    self._zku._zk.remove(path, callback);
  }
  self._zku.onConnection(onConnection);
};


ZkOrca.prototype._doubleBarrierEnter = function(barrierPath, clientCount, timeout, callback) {
  var self = this,
      readyPath = sprintf('%s/ready', barrierPath),
      myPath = sprintf('%s/%s-', barrierPath, uuid.v4()),
      ready = false,
      timeoutTimer,
      doneCallback;

  doneCallback = _.once(callback);

  if (timeout > 0) {
    timeoutTimer = setTimeout(function() {
      ready = true;
      doneCallback(new TimeoutException());
    }, timeout);
  }

  function filterChildren(children) {
    return _.filter(children, function(child) {
      return child !== 'ready';
    });
  }

  log.trace1('Registering double barrier', { myPath: myPath });

  function onConnection(err) {
    if (err) {
      log.trace1('Error while waiting for connection (doubleBarrier)', { err: err });
      callback(err);
      return;
    }
    function internalEnter(path) {
      var found;
      if (ready) {
        return;
      }
      async.auto({
        'getChildren': function (callback) {
          self._zku._zk.getChildren(barrierPath, function watcher(event) {
            if (event.name === 'NODE_CHILDREN_CHANGED') {
              _.delay(internalEnter.bind(self), 0, path);
            }
          }, callback);
        },
        'checkForChildren': ['getChildren', function (callback, results) {
          var found = filterChildren(results.getChildren[0]).length == clientCount;
          if (found) {
            ready = true;
            if (timeoutTimer) {
              clearTimeout(timeoutTimer);
            }
            self._zku._zk.create(readyPath, null, zookeeper.CreateMode.EPHEMERAL, function (err) {
              log.trace1('created ready node (doubleBarrier)');
              callback(err);
              doneCallback(null, path);
            });
          } else {
            callback();
          }
        }]
      });
    }
    function pathExists(callback) {
      self._zku._zk.mkdirp(barrierPath, callback);
    }
    function checkForReadyPath(callback) {
      self._zku._zk.exists(readyPath, callback);
    }
    function register(callback) {
      self._zku._zk.create(myPath, null, zookeeper.CreateMode.EPHEMERAL_SEQUENTIAL, callback);
    }
    self._zku._zk.exists(readyPath, function(err, stat) {
      if (err) {
        return callback(err);
      }
      if (stat) {
        return callback();
      }
      async.auto({
        'pathExists': pathExists,
        'readyPath': ['pathExists', checkForReadyPath],
        'register': ['readyPath', register],
        'internal': ['register', function(callback, status) {
          internalEnter(status.register);
          callback();
        }]
      });
    });
  }
  self._zku.onConnection(onConnection);
  return myPath;
};


ZkOrca.prototype._doubleBarrierLeave = function(path, callback) {
  var self = this;
  function onConnection(err) {
    if (err) {
      log.trace1('Error while waiting for connection (doubleBarrier)', {err: err});
      callback(err);
      return;
    }
    self._zku._zk.remove(path, -1, callback);
  }
  self._zku.onConnection(onConnection);
};


/**
 * Atomically decrement and return a long.
 * @param {String} name A zookeeper path.
 * @param {String} txnId A transaction ID.
 * @param {Function} callback A callback called with (err, value);
 */
ZkOrca.prototype.decrementAndGet = function(name, txnId, callback) {
  this.decrementAndGetBy(name, 1, txnId, callback);
};


/**
 * Atomically decrement a long by a certain value and return it.
 * @param {String} name A zookeeper path.
 * @param {Number} value A integer value by which to decrease the long.
 * @param {String} txnId A transaction ID.
 * @param {Function} callback A callback called with (err, value);
 */
ZkOrca.prototype.decrementAndGetBy = function(name, value, txnId, callback) {
  this.incrementAndGetBy(name, (-value), txnId, callback);
};


/**
 * Atomically increment and return a long.
 * @param {String} name A zookeeper path.
 * @param {String} txnId A transaction ID.
 * @param {Function} callback A callback called with (err, value);
 */
ZkOrca.prototype.incrementAndGet = function(name, txnId, callback) {
  this.incrementAndGetBy(name, 1, txnId, callback);
};


/**
 * Atomically increment a long by a certain value and return it.
 * @param {String} name A zookeeper path.
 * @param {Number} value A integer value by which to increase the long.
 * @param {String} txnId A transaction ID.
 * @param {Function} callback A callback called with (err, value);
 */
ZkOrca.prototype.incrementAndGetBy = function(name, value, txnId, callback) {
  var self = this,
      dataFilename, lockFilename;

  if (name[name.length - 1] === '/') {
    callback(new Error('Path name may not end in \'/\''));
    return;
  }

  dataFilename = path.join('/', self._name, 'data', name);
  lockFilename = path.join('/', self._name, 'locks', name);

  function addUnlockToCallback(callback) {
    return function() {
      var args = arguments;

      self._zku.unlock(lockFilename, function(err) {
        if (err) {
          log.error('Failed to unlock.', {txnId: txnId, err: err, lockName: lockFilename});
        } else {
          log.debug('Lock unlocked.', {txnId: txnId, lockName: lockFilename});
        }
        callback.apply(null, args);
      });
    };
  }

  callback = addUnlockToCallback(callback);
  callback = (function(callback) {
    var timerId = setTimeout(function onconnectTimeout() {
      var error = new Error("Timed out after " + self._timeout);
      callback(error);
    }, self._timeout);
    return function(err) {
      if (err) {
        log.trace1('Error while waiting for connection', { err: err });
      }
      clearTimeout(timerId);
      callback.apply(null, arguments);
    };
  })(callback);
  callback = _.once(callback);


  function onConnection(err) {
    if (err) {
      log.trace1('Error while waiting for connection (incrementAndGetBy)', {err: err});
      callback(err);
      return;
    }

    async.auto({
      lock: function(callback) {
        self._zku.lock(lockFilename, txnId, function(err) {
          callback(err);
        });
      },

      exists: ['lock', function(callback) {
        self._zku._zk.exists(dataFilename, callback);
      }],

      mkdir: ['exists', function(callback, result) {
        var exists = result.exists;

        if (exists) {
          callback();
          return;
        }

        self._zku._zk.mkdirp(path.dirname(dataFilename), callback);
      }],

      getData: ['mkdir', function(callback) {
        self._zku._zk.getData(dataFilename, null, function(err, data, stat) {
          if (err) {
            data = 0;
            self._zku._zk.create(dataFilename, new Buffer('' + data), function(err) {
              if (err) {
                callback(err);
                return;
              }
              callback(null, data);
            });
            return;
          }

          data = data ? parseInt(data.toString(), 10) : 0;
          callback(null, data);
        });
      }],

      setData: ['getData', function(callback, result) {
        var data = result.getData,
            newValue;

        if (!data) {
          data = 0;
        }

        newValue = data + value;
        self._zku._zk.setData(dataFilename, new Buffer('' + newValue), -1, function(err, stat) {
          if (err) {
            callback(err);
            return;
          }

          callback(null, newValue);
        });
      }]
    }, function(err, result) {
      callback(err, result.setData);
    });
  }

  self._zku.onConnection(onConnection);
};


/**
 * Get the specified long.
 * @param {String} name A zookeeper path.
 * @param {Number?} value An optional value to default to if the long has not yet been instantiated. If provided,
 * this function will attempt to initialize the value, otherwise it will error if the value at the path has not been
 * instantiated previously.
 * @param {Function} callback A callback called with (err, value);
 */
ZkOrca.prototype.get = function(name, value, callback) {
  var self = this,
      dataFilename;

  if (name[name.length - 1] === '/') {
    callback(new Error('Path name may not end in \'/\''));
    return;
  }

  if (!callback && typeof value === 'function') {
    callback = value;
    value = null;
  }

  dataFilename = path.join('/', self._name, 'data', name);

  function onConnectionNoValue(err) {
    if (err) {
      log.trace1('Error while waiting for connection (get)', {err: err});
      callback(err);
      return;
    }

    self._zku._zk.getData(dataFilename, null, function(err, data, stat) {
      if (err) {
        callback(err);
        return;
      }

      data = data ? parseInt(data.toString(), 10) : 0;

      callback(null, data);
    });
  }

  function onConnectionValue(err) {
    if (err) {
      log.trace1('Error while waiting for connection (get)', {err: err});
      callback(err);
      return;
    }

    async.auto({
      getData: function(callback) {
        self._zku._zk.getData(dataFilename, null, function(err, data, stat) {
          if (err) {
            callback(null, false);
            return;
          }

          data = data ? parseInt(data.toString(), 10) : 0;

          callback(null, data);
        });
      },

      mkdir: ['getData', function(callback, result) {
        var getDataSuccess = result.getData;

        if (getDataSuccess) {
          callback();
          return;
        }

        self._zku._zk.mkdirp(path.dirname(dataFilename), callback);
      }],

      create: ['mkdir', function(callback, result) {
        var getDataSuccess = result.getData;

        if (getDataSuccess) {
          callback();
          return;
        }

        self._zku._zk.create(dataFilename, new Buffer('' + value), function(err) {
          if (err) {
            callback(err);
            return;
          }

          callback(null, value);
        });
      }]
    }, function(err, result) {
      var data;

      if (err) {
        callback(err);
        return;
      }

      data = result.getData || result.create;

      callback(null, data);
    });


  }

  self._zku.onConnection(value !== null && value !== undefined ? onConnectionValue : onConnectionNoValue);
};


/**
 *
 * @param callback
 */
ZkOrca.prototype.close = function(callback) {
  this._zku.close(callback);
};

