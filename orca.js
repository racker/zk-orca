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


/**
 *
 * @param accountKey
 * @param zoneName
 * @param agentId
 * @param connectionGuid
 */
ZkOrca.prototype.addNode = function(accountKey, zoneName, agentId, connGuid, callback) {
  var self = this,
      connPath = sprintf('/%s/%s/%s/connections/%s-%s', this._name, accountKey, zoneName, agentId, connGuid);
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


/**
 *
 * @param callback
 */
ZkOrca.prototype.close = function(callback) {
  this._zku.close(callback);
};

