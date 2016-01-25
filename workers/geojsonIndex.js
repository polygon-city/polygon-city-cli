var _ = require('lodash');
var Queue = require('bull');
var Promise = require('bluebird');
var chalk = require('chalk');
var DOMParser = require('xmldom').DOMParser;
var domParser = new DOMParser();
var xmldom2xml = require('xmldom-to-xml');
var proj4 = require('proj4');
var turf = require('turf');
var path = require('path');
var fs = Promise.promisifyAll(require('fs-extra'));
var citygmlPoints = require('citygml-points');
var Redis = require('ioredis');

// TODO: Make this work after a crash / on resume as it currently only stores
// footprints from the current session

var redisHost = process.env.REDIS_PORT_6379_TCP_ADDR || '127.0.01';
var redisPort = process.env.REDIS_PORT_6379_TCP_PORT || 6379;

var redis = new Redis(redisPort, redisHost);

var geojsonIndexQueue = Queue('geojson_index_queue', redisPort, redisHost);

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

var createFootprintIndex = function(id, outputPath) {
  var footprints = [];
  var features = [];

  // Get footprints from Redis
  return redis.lrange('polygoncity:job:' + id + ':footprints', 0, -1).then(function(results) {
    results.forEach(function(footprint) {
      footprints.push(JSON.parse(footprint));
    });

    for (var i = 0; i < footprints.length; i++) {
      features.push(footprints[i]);
    }

    var featureCollection = turf.featurecollection(features);

    var _outputPath = path.join(outputPath, 'index.geojson');

    console.log('Number of GeoJSON footprints:', footprints.length);

    return fs.outputFileAsync(_outputPath, JSON.stringify(featureCollection));
  });
};

var existingIndex;
var proj4def;
var projection;

var worker = function(job, done) {
  var data = job.data;

  if (!proj4def && data.proj4def) {
    proj4def = data.proj4def;
    projection = proj4.defs('EPSG:ORIGIN', proj4def);
  }

  var id = data.id;
  var outputPath = data.outputPath;
  var buildingId = data.buildingId;
  var xml = data.xml;

  var xmlDOM = domParser.parseFromString(xml);

  // Add GeoJSON outline of footprint (if available)
  var footprint = getFootprint(xmlDOM, buildingId);

  if (footprint) {
    // Add to Redis list
    redis.rpush('polygoncity:job:' + id + ':footprints', JSON.stringify(footprint)).then(function() {
      // Increment final job count
      redis.hincrby('polygoncity:job:' + id, 'final_job_count', 1).then(function(finalJobCount) {
        // Get final building count, if it exists
        redis.hget('polygoncity:job:' + id, 'buildings_count_final').then(function(result) {
          // All jobs completed
          if (result == finalJobCount) {
            // Compile GeoJSON index of footprints
            createFootprintIndex(id, outputPath).then(function() {
              console.log(chalk.green('Saved GeoJSON index:', outputPath));

              // Mark job as complete
              redis.hset('polygoncity:job:' + id, 'completed', 1).then(function() {
                done();
              });
            }).catch(function(err) {
              console.error(err);
              done(err);
            });
          // Still jobs left to go
          } else {
            done();
          }
        });
      });
    });
  } else {
    console.log(chalk.red('Unable to find footprint for building:', buildingId));
  }
};

module.exports = worker;
