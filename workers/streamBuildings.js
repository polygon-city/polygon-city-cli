var _ = require('lodash');
var Promise = require('bluebird');
var kue = require('kue');
var fs = require('fs');
var sax = require('sax');
var saxpath = require('saxpath');
var DOMParser = require('xmldom').DOMParser;
var domParser = new DOMParser();
var chalk = require('chalk');
var Redis = require('ioredis');

var citygmlPoints = require('citygml-points');

// Setting strict to true causes all sorts of errors with non-XML files
//
// TODO: Work out a way to properly handle these errors
var strict = true;

var redisHost = process.env.REDIS_PORT_6379_TCP_ADDR || '127.0.01';
var redisPort = process.env.REDIS_PORT_6379_TCP_PORT || 6379;

var redis = new Redis(redisPort, redisHost);

var queue = kue.createQueue({
  redis: {
    port: redisPort,
    host: redisHost,
  }
});

var exiting = false;
var readStream;

var worker = function(job, done) {
  var dom = require('domain').create();

  dom.on('error', function(err) {
    // Clean up
    saxParser.end();
    readStream.close();

    saxStream = undefined;

    done(err);
  });

  var data = job.data;
  var id = data.id;
  var prefix = data.prefix || '';
  var path = data.inputPath;

  // Create job state hash
  redis.hset('polygoncity:job:' + id, 'id', id);

  var saxParser = sax.createStream(strict, {
    position: true
  });

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

  // var count = 0;
  saxStream.on('match', function(xml) {
    if (exiting) {
      return;
    }

    // if (++count > 100) {
    //   return;
    // }

    var xmlDOM = domParser.parseFromString(xml);

    var buildingId = xmlDOM.firstChild.getAttribute('gml:id') || UUID.v4();

    var prefixedId;

    if (prefix) {
      prefixedId = prefix + buildingId;
    }

    // Skip building if already in streamed set
    redis.sismember('polygoncity:job:' + id + ':streamed_buildings', buildingId).then(function(result) {
      if (result === 1) {
        return;
      }

      // Append data onto job payload
      var newData = _.extend({}, data, {
        buildingId: (prefixedId) ? prefixedId : buildingId,
        buildingIdOriginal: buildingId,
        xml: xml
      });

      // console.log(buildingId);

      // Add building to processing queue
      queue.create('repair_building_queue', newData).save(function() {
        // Add building ID to streamed buildings set
        redis.sadd('polygoncity:job:' + id + ':streamed_buildings', buildingId);

        // Increment building count
        redis.hincrby('polygoncity:job:' + id, 'buildings_count', 1);
      });
    });
  });

  saxStream.on('end', function() {
    if (exiting) {
      return;
    }

    console.log('Stream ended');

    // Wait a moment for left-overs
    // TODO: Work out how to be more deliberate about knowing when all the
    // matches have been retrieved from the XML
    setTimeout(function() {
      // Update final building count
      redis.hget('polygoncity:job:' + id, 'buildings_count').then(function(result) {
        console.log("Building count:", result);

        redis.hset('polygoncity:job:' + id, 'buildings_count_final', result);

        // Remove streamed buildings set
        redis.del('polygoncity:job:' + id + ':streamed_buildings');

        done();
      });
    }, 100);
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

var onExit = function() {
  console.log(chalk.red('Exiting streamBuildings worker...'));
  exiting = true;

  readStream.pause();
};

process.on('SIGINT', onExit);

module.exports = worker;
