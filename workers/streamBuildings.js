var _ = require('lodash');
var Promise = require('bluebird');
var Queue = require('bull');
var fs = require('fs');
var sax = require('sax');
var saxpath = require('saxpath');
var DOMParser = require('xmldom').DOMParser;
var domParser = new DOMParser();
var xmldom2xml = require('xmldom-to-xml');
var proj4 = require('proj4');
var turf = require('turf');
var path = require('path');
var fs = Promise.promisifyAll(require('fs-extra'));
var chalk = require('chalk');
var Redis = require('ioredis');

var citygmlPoints = require('citygml-points');

// TODO: Split out GeoJSON footprint process into a worker as it needs to know
// about various things like the path to the converted file, etc
//
// Need to work out how to know when the processing is finished so the final
// GeoJSON file can be compiled and saved from all the footprints

// Setting strict to true causes all sorts of errors with non-XML files
//
// TODO: Work out a way to properly handle these errors
var strict = true;

var redisHost = process.env.REDIS_PORT_6379_TCP_ADDR || '127.0.01';
var redisPort = process.env.REDIS_PORT_6379_TCP_PORT || 6379;

var redis = new Redis(redisPort, redisHost);

var repairBuildingQueue = Queue('repair_building_queue', redisPort, redisHost);

var getFootprint = function(xmlDOM, id) {
  // Find ground surfaces
  var groundSurfaces = xmldom2xml(xmlDOM.getElementsByTagName('bldg:GroundSurface'));

  if (!groundSurfaces && groundSurfaces.length === 0) {
    return false;
  }

  var points;
  var polygons = [];
  for (var i = 0; i < groundSurfaces.length; i++) {
    points = citygmlPoints(groundSurfaces[i]).map((point) => {
      return proj4('EPSG:ORIGIN').inverse([point[0], point[1]]);
    });

    polygons.push(turf.polygon([points], {
      id: id
    }));
  }

  var featureCollection = turf.featurecollection(polygons);
  var polygon = turf.merge(featureCollection);

  return polygon;
};

var createFootprintIndex = function(footprints, outputPath) {
  var features = [];

  for (var i = 0; i < footprints.length; i++) {
    features.push(footprints[i]);
  }

  var featureCollection = turf.featurecollection(features);

  var _outputPath = path.join(outputPath, 'index.geojson');

  return fs.outputFileAsync(_outputPath, JSON.stringify(featureCollection));
};

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

  var footprints = [];

  var data = job.data;
  var id = data.id;
  var path = data.inputPath;
  var outputPath = data.outputPath;

  // Create job state hash
  redis.hset('polygoncity:job:' + id, 'id', id);

  var proj4def = data.proj4def;
  var projection = proj4.defs('EPSG:ORIGIN', proj4def);

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

  // saxParser.on('opentag', function(node) {
  //   if (!node.name || node.name != 'bldg:Building') {
  //     return;
  //   }
  // });
  //
  // var lastCloseBytes;
  // saxParser.on('closetag', function(node) {
  //   if (node != 'bldg:Building') {
  //     return;
  //   }
  // });

  saxParser.on('end', function() {
    console.error("Parser ended");
  });

  var saxStream = new saxpath.SaXPath(saxParser, '//bldg:Building');

  saxStream.on('match', function(xml) {
    var xmlDOM = domParser.parseFromString(xml);

    var buildingId = xmlDOM.firstChild.getAttribute('gml:id') || UUID.v4();

    // Skip building if already in streamed set
    redis.sismember('polygoncity:job:' + id + ':streamed_buildings', buildingId).then(function(result) {
      if (result === 1) {
        return;
      }

      // Add GeoJSON outline of footprint (if available)
      var footprint = getFootprint(xmlDOM, buildingId);

      if (footprint) {
        footprints.push(footprint);
      } else {
        console.log('Unable to find footprint for building:', buildingId);
      }

      // Append data onto job payload
      var newData = _.extend({}, data, {
        buildingId: buildingId,
        xml: xml
      });

      // Add building to processing queue
      repairBuildingQueue.add(newData).then(function() {
        // Add building ID to streamed buildings set
        redis.sadd('polygoncity:job:' + id + ':streamed_buildings', buildingId);

        // Increment building count
        redis.hincrby('polygoncity:job:' + id, 'buildings_count', 1);
      });
    });
  });

  saxStream.on('end', function() {
    console.log('Stream ended');

    // Compile GeoJSON index of footprints
    createFootprintIndex(footprints, outputPath).then(function() {
      console.log(chalk.green('Saved GeoJSON index'));

      // Update final building count
      redis.hget('polygoncity:job:' + id, 'buildings_count').then(function(result) {
        redis.hset('polygoncity:job:' + id, 'buildings_count_final', result);

        // Remove streamed buildings set
        redis.del('polygoncity:job:' + id + ':streamed_buildings');

        done();
      });
    }).catch(function(err) {
      console.error(err);
      done(err);
    });
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
