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
cxn.removeNode('accountKey', 'zoneName', 'agentId', 'connection guid');

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
var cxns = {}; // urls -> ZkCxn


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
 *
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
 *
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
ZkOrca.prototype.monitor = function(accountKey, zoneName, callback) {
  var self = this,
      path = sprintf('/%s/%s/%s/connections', this._name, accountKey, zoneName);
  self._zku.onConnection(function(err) {
    if (err) {
      log.trace1('Error while waiting for connection (monitor)', { err: err });
      callback(err);
      return;
    }
    async.auto({
      'path': function(callback) {
         if (self._zku._cxnState !== self._zku.cxnStates.CONNECTED) {
           log.trace1('Error observed on path creation', { error: err });
           return;
         }
         try {
           self._zku._zk.mkdirp(path, callback);
         } catch (err) {
           callback(err);
         }
      },
      'watch': ['path', function(callback) {
        log.trace1('Starting watch', { accountKey: accountKey, zoneName: zoneName });
        self._zku._zk.getChildren(path, function(event) {
          self.emit('zone:' + accountKey + ':' + zoneName, event);
        }, callback);
      }]
    });
  }, callback);
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
      connPath = sprintf('%s/connections/%s-%s', self._basePath(accountKey, zoneName), agentId, connGuid);
  self._zku.onConnection(function(err) {
    if (err) {
      log.trace1('Error while waiting for connection (monitor)', { err: err });
      callback(err);
      return;
    }
    async.auto({
      'path': function(callback) {
        log.trace1('Creating node', { path: connPath });
        self._zku._zk.create(connPath, null, zookeeper.CreateMode.EPHEMERAL, callback);
      }
    }, callback);
  });
};


ZkOrca.prototype._doubleBarrierEnter = function(barrierPath, clientCount, timeout, callback) {
  var self = this,
      readyPath = sprintf('%s/ready', barrierPath),
      myPath = sprintf('%s/%s', barrierPath, uuid.v4());

  log.trace1('Registering double barrier', { myPath: myPath });

  function onConnection(err) {
    if (err) {
      log.trace1('Error while waiting for connection (doubleBarrier)', { err: err });
      callback(err);
      return;
    }
    function internalEnter() {
      var count = 0,
          found = false;
      async.during(
        function(callback) {
          if (found) { return callback(null, false); }
          self._zku._zk.getChildren(barrierPath, function(err, children) {
            if (err) { return callback(err); }
            count = children.length;
            if (count < clientCount) {
              setTimeout(0, callback, count < clientCount);
            } else {
              callback(null, count < clientCount);
            }
          });
        },
        function(callback) {
          self._zku._zk.getChildren(barrierPath, function(err, children) {
            if (err) { return callback(err); }
            found = children.length == clientCount;
            callback();
          });
        }, function(err) {
        if (err) {
          callback(err);
          return;
        }
        self._zku._zk.create(readyPath, null, zookeeper.CreateMode.EPHEMERAL, function(err) {
          if (err) {
            if (err.name !== 'NODE_EXISTS') {
              callback(err);
            }
          }
          log.trace1('created ready node (doubleBarrier)');
          callback();
        });
      });
    }
    function pathExists(callback) {
      self._zku._zk.mkdirp(barrierPath, callback);
    }
    function checkForReadyPath(callback) {
      self._zku._zk.exists(readyPath, callback);
    }
    function register(callback) {
      self._zku._zk.create(myPath, null, zookeeper.CreateMode.EPHEMERAL, callback);
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
        'internal': ['register', function(callback) {
          internalEnter();
          callback();
        }]
      });
    });
  }
  self._zku.onConnection(onConnection);
  return myPath;
};


/**
 *
 * @param callback
 */
ZkOrca.prototype.close = function(callback) {
  this._zku.close(callback);
};

