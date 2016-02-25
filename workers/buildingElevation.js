var _ = require('lodash');
var Promise = require('bluebird');
var kue = require('kue');
var chalk = require('chalk');
var DOMParser = require('xmldom').DOMParser;
var domParser = new DOMParser();
var xmldom2xml = require('xmldom-to-xml');
var proj4 = require('proj4');
var request = Promise.promisify(require('request'), {multiArgs: true});
var citygmlPoints = require('citygml-points');
var Redis = require('ioredis');

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

var worker = function(job, done) {
  var data = job.data;

  var id = data.id;
  var buildingId = data.buildingId;
  var xml = data.xml;
  var xmlDOM = domParser.parseFromString(xml);
  var polygons = data.polygons;

  var valhallaKey = data.mapzenKey;
  var proj4def = data.proj4def;

  if (!valhallaKey) {
    var err = new Error('No Valhalla key was provided to retrieve elevation');
    console.error(err);
    failBuilding(id, buildingId, done, err);
    return;
  }

  // Origin point
  var origin;

  var zUP = true;

  try {
    // Find highest ground surface for elevation origin
    var groundSurfaces = xmldom2xml(xmlDOM.getElementsByTagName('bldg:GroundSurface'));

    if (groundSurfaces && groundSurfaces.length > 0) {
      var maxGroundElevation;
      var maxGroundIndex;

      groundSurfaces.forEach(function(groundSurface, gsIndex) {
        var gsPoints = citygmlPoints(groundSurface);

        gsPoints.forEach(function(gsPoint) {
          if (!maxGroundElevation || gsPoint[2] > maxGroundElevation) {
            maxGroundElevation = gsPoint[2];
            maxGroundIndex = gsIndex;

            return false;
          }
        });
      });
    }
  } catch(err) {
    console.error(err);
    failBuilding(id, buildingId, done, err);
    return;
  }

  // Vertical can be either Y (1) or Z (2)
  var verticalIndex = (zUP) ? 2 : 1;

  // Horizontal can be either X (0) or Y (1)
  var horizontalIndex = (zUP) ? 0 : 1;

  var vertMin;

  polygons.forEach(function(polygon) {
    // Find minimum on vertical axis
    polygon.forEach(function(point) {
      if (!vertMin) {
        vertMin = point[verticalIndex];
        return;
      }

      if (point[verticalIndex] < vertMin) {
        vertMin = point[verticalIndex];
        return;
      }
    });
  });

  // Collect points that share minimum vertical values
  var vertMinPoints = [];
  polygons.forEach(function(polygon) {
    polygon.forEach(function(point) {
      vertMinPoints = _.unique(vertMinPoints.concat(_.filter(polygon, function(point) {
        return (point[verticalIndex] === vertMin);
      })));
    });
  });

  // Find point with minimum on alternate horizontal axis
  vertMinPoints.forEach(function(point) {
    if (!origin) {
      origin = _.clone(point);
      return;
    }

    if (point[horizontalIndex] < origin[horizontalIndex]) {
      origin = _.clone(point);
      return;
    }
  });

  var projection = proj4.defs('EPSG:ORIGIN', proj4def);

  // Convert coordinates from SRS to WGS84 [lon, lat]
  var coords = proj4('EPSG:ORIGIN').inverse([origin[0], origin[1]]);

  var queueName = (data.wofEndpoint) ? 'whos_on_first_queue' : 'building_obj_queue';

  // Skip external elevation API if ground elevation is provided
  if (maxGroundElevation) {
    // Append data onto job payload
    _.extend(data, {
      origin: origin,
      originWGS84: coords,
      elevation: maxGroundElevation
    });

    queue.create(queueName, data).save(function() {
      done();
    });
  } else {
    var url = data.elevationEndpoint + '/height?json={%22shape%22:[{%22lat%22:' + coords[1] + ',%22lon%22:' + coords[0] + '}]}&api_key=' + valhallaKey;

    // Retreive elevation via API
    request(url).then(function(response) {
      var res = response[0];
      var body = response[1];

      if (res.statusCode != 200) {
        var err = new Error('Unexpected elevation data response, HTTP: ' + res.statusCode);
        console.error(err);
        console.log(body);
        failBuilding(id, buildingId, done, err);
        return;
      }

      try {
        var bodyJSON = JSON.parse(body);

        if (!bodyJSON.height || bodyJSON.height.length === 0) {
          var err = new Error('Elevation values not present in API response');
          console.error(err);
          console.log(body);
          failBuilding(id, buildingId, done, err);
          return;
        }

        var elevation = bodyJSON.height[0];

        // Append data onto job payload
        _.extend(data, {
          origin: origin,
          originWGS84: coords,
          elevation: elevation
        });

        queue.create(queueName, data).save(function() {
          done();
        });
      } catch(err) {
        var err = new Error('Unexpected elevation data response' + ((err.message) ? ': ' + err.message : ''));
        console.error(err);
        console.log(body);
        failBuilding(id, buildingId, done, err);
        return;
      }
    }).catch(function(err) {
      if (err) {
        var err = new Error('Unable to retrieve elevation data' + ((err.message) ? ': ' + err.message : ''));
        console.error(err);
        failBuilding(id, buildingId, done, err);
        return;
      }
    });
  }
};

var failBuilding = function(id, buildingId, done, err) {
  // Add building ID to failed buildings set
  redis.rpush('polygoncity:job:' + id + ':buildings_failed', buildingId).then(function() {
    // Increment failed building count
    return redis.hincrby('polygoncity:job:' + id, 'buildings_count_failed', 1).then(function() {
      // Even though the model failed, don't pass on error otherwise job
      // will fail and prevent overall completion (due to a failed job)
      done();
    });
  });
};

var onExit = function() {
  console.log(chalk.red('Exiting buildingElevation worker...'));
  exiting = true;
};

process.on('SIGINT', onExit);

module.exports = worker;
