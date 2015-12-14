var _ = require('lodash');
var Queue = require('bull');
var fs = require('fs');
var sax = require('sax');
var saxpath = require('saxpath');
var DOMParser = require('xmldom').DOMParser;
var domParser = new DOMParser();

// Setting strict to true causes all sorts of errors with non-XML files
//
// TODO: Work out a way to properly handle these errors
var strict = true;

var redisHost = process.env.REDIS_PORT_6379_TCP_ADDR || '127.0.01';
var redisPort = process.env.REDIS_PORT_6379_TCP_PORT || 6379;

var repairBuildingQueue = Queue('repair_building_queue', redisPort, redisHost);

var worker = function(job, done) {
  var dom = require('domain').create();

  dom.on('error', function(err) {
    // Clean up
    saxParser.end();
    readStream.close();

    saxStream = undefined;

    done(err);
  });

  var readStream;

  var data = job.data;
  var path = data.inputPath;

  var saxParser = sax.createStream(strict);

  var streamErrorHandler = function (err) {
    // Sax-js requires you to throw an error here else it continues parsing
    //
    // See: https://github.com/isaacs/sax-js/blob/master/lib/sax.js#L197
    throw err;
  };

  saxParser.on('error', streamErrorHandler);

  saxParser.on('end', function() {
    console.error("Parser ended");
  });

  var saxStream = new saxpath.SaXPath(saxParser, '//bldg:Building');

  saxStream.on('match', function(xml) {
    var xmlDOM = domParser.parseFromString(xml);

    var buildingId = xmlDOM.firstChild.getAttribute('gml:id') || UUID.v4();

    // Append data onto job payload
    var newData = _.extend({}, data, {
      buildingId: buildingId,
      xml: xml
    });

    // TODO: Add building to processing queue
    repairBuildingQueue.add(newData);
  });

  saxStream.on('end', function() {
    console.log('Stream ended');
    done();
  });

  // Wrap in a domain to catch errors
  //
  // TODO: This is considered bad practice so a new approach is required
  // See: https://nodejs.org/api/domain.html#domain_warning_don_t_ignore_errors
  dom.run(function() {
    readStream = fs.createReadStream(path);
    readStream.pipe(saxParser);
  });
};

module.exports = worker;
