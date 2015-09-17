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
      isPrimary = sorted[0].lastIndexOf(connTuple[2]);
    }
    callback(null, isPrimary);
  });
};


ZkOrca.prototype._basePath = function(accountKey, zoneName) {
  return sprintf('/%s/%s', accountKey, zoneName);
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
        'getChildren': function(callback) {
          self._zku._zk.getChildren(barrierPath, function watcher(event) {
            if (event.name === 'NODE_CHILDREN_CHANGED') {
              _.delay(internalEnter.bind(self), 0, path);
            }
          }, callback);
        },
        'checkForChildren': ['getChildren', function(callback, results) {
          var found = filterChildren(results.getChildren[0]).length == clientCount;
          if (found) {
            ready = true;
            if (timeoutTimer) {
              clearTimeout(timeoutTimer);
            }
            self._zku._zk.create(readyPath, null, zookeeper.CreateMode.EPHEMERAL, function(err) {
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
 *
 * @param callback
 */
ZkOrca.prototype.close = function(callback) {
  this._zku.close(callback);
};

