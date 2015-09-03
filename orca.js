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
 * @description
 *
 * Module interface:
 * - ZkCXN = exports.getCxn(zkUrls, cxnTimeout)
 * - exports.shutdown(callback)
 *
 * ZkCxn:
 * - lock(name, txnId, callback)
 * - unlock(name, callback)
 *
 * Public methods on ZkCxn you probably do not need to call:
 * - onConnection(callback)
 * - close(callback)
 */

/**
 * @example
var zkorca = require('zk-orca');
var cxn = zkorca.getCxn({ urls: ['127.0.0.1:2181']});
cxn.monitor('zoneName')
cxn.on('zone:zoneName', onZoneChange);
 */

var util = require('util');
var events = require('events');
var async = require('async');
var _ = require('underscore');
var zkUltraCxn = require('zk-ultralight').getCxn;
var log = require('logmagic').local('zk-ultralight');


// used for onConnection callbacks which is both lock and unlock
var DEFAULT_TIMEOUT = 16000;

var cxns = {}; // urls -> ZkCxn


exports.getCxn = function getCxn(options) {
  var urls = options.urls.join(',');
  options = _.extend(options, { urls: urls });
  log.trace1('getCxn');
  if (!cxns[urls]) {
    cxns[urls] = new ZkOrca(options);
  }
  return cxns[urls];
};


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


/*
 * @constructor
 * @params {String} urls A comma-delimited array of <ZK host:port>s.
 * @params {?Number} timeout A timeout.
 */
function ZkOrca(opts) {
  opts = opts || {};
  this._urls = opts.urls;
  this._timeout = opts.timeout || DEFAULT_TIMEOUT;
}


ZkOrca.prototype.close = function(callback) {
  _.delay(callback, 0);
};

