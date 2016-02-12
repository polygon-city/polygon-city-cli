var _ = require('lodash');
var Promise = require('bluebird');
var Queue = require('bull');
var chalk = require('chalk');
var DOMParser = require('xmldom').DOMParser;
var domParser = new DOMParser();
var xmldom2xml = require('xmldom-to-xml');
var proj4 = require('proj4');
var request = Promise.promisify(require('request'), {multiArgs: true});
var citygmlPoints = require('citygml-points');

var redisHost = process.env.REDIS_PORT_6379_TCP_ADDR || '127.0.01';
var redisPort = process.env.REDIS_PORT_6379_TCP_PORT || 6379;

var buildingObjQueue = Queue('building_obj_queue', redisPort, redisHost);
var whosOnFirstQueue = Queue('whos_on_first_queue', redisPort, redisHost);

var exiting = false;

var worker = function(job, done) {
  var data = job.data;

  var xml = data.xml;
  var xmlDOM = domParser.parseFromString(xml);
  var polygons = data.polygons;

  var valhallaKey = data.mapzenKey;
  var proj4def = data.proj4def;

  if (!valhallaKey) {
    var err = new Error('No Valhalla key was provided to retrieve elevation');
    console.error(err);
    done(err);
    return;
  }

  // Origin point
  var origin;

  var zUP = true;

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

  var queue = (data.wofEndpoint) ? whosOnFirstQueue : buildingObjQueue;

  // Skip external elevation API if ground elevation is provided
  if (maxGroundElevation) {
    // Append data onto job payload
    _.extend(data, {
      origin: origin,
      originWGS84: coords,
      elevation: maxGroundElevation
    });

    queue.add(data).then(function() {
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
        done(err);
        return;
      }

      try {
        var bodyJSON = JSON.parse(body);

        if (!bodyJSON.height || bodyJSON.height.length === 0) {
          var err = new Error('Elevation values not present in API response');
          console.error(err);
          done(err);
          return;
        }

        var elevation = bodyJSON.height[0];

        // Append data onto job payload
        _.extend(data, {
          origin: origin,
          originWGS84: coords,
          elevation: elevation
        });

        queue.add(data).then(function() {
          done();
        });
      } catch(err) {
        var err = new Error('Unexpected elevation data response' + ((err.message) ? ': ' + err.message : ''));
        console.error(err);
        done(err);
        return;
      }
    }).catch(function(err) {
      if (err) {
        var err = new Error('Unable to retrieve elevation data' + ((err.message) ? ': ' + err.message : ''));
        console.error(err);
        done(err);
        return;
      }
    });
  }
};

var onExit = function() {
  console.log(chalk.red('Exiting buildingElevation worker...'));
  exiting = true;
  // process.exit(1);
};

process.on('SIGINT', onExit);

module.exports = worker;
