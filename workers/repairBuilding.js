var Promise = require('bluebird');
var kue = require('kue');
var _ = require('lodash');
var chalk = require('chalk');

var citygmlPolygons = require('citygml-polygons');
var citygmlBoundaries = require('citygml-boundaries');
var citygmlPoints = require('citygml-points');
var citygmlValidateShell = Promise.promisify(require('citygml-validate-shell'));

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

var repair = function(polygons, validationResults) {
  // Repair CityGML
  // TODO: Revalidate and repair after each repair, as geometry will change
  var polygonsCopy = _.clone(polygons);

  // Face flipping
  var flipFaces = [];

  validationResults.forEach(function(vError) {
    // Should always be an error, but check anyway
    if (!vError || !vError[0]) {
      return;
    }

    // Failure indexes, for repair
    var vIndices = vError[1];

    // Output validation error name
    // TODO: Halt conversion on particularly bad validation errors
    switch (vError[0].message.split(':')[0]) {
      case 'GE_S_POLYGON_WRONG_ORIENTATION':
      case 'GE_S_ALL_POLYGONS_WRONG_ORIENTATION':
        // TODO: Work out why reversing the vertices doesn't flip the
        // normal so we can fix things that way
        vIndices.forEach(function(vpIndex) {
          var points = polygonsCopy[vpIndex];

          // REMOVED: Until it can be worked out why reversing doesn't
          // actually flip the normal in this case (it should)
          // polygonsCopy[vpIndex].reverse();

          // Add face to be flipped
          flipFaces.push(vpIndex);
        });

        break;
    }
  });

  return Promise.resolve({
    polygons: polygonsCopy,
    flipFaces: flipFaces
  });
};

var worker = function(job, done) {
  var data = job.data;
  var id = data.id;
  var xml = data.xml;
  var buildingId = data.buildingId;

  var polygonsGML = citygmlPolygons(xml);
  var allPolygons = [];

  polygonsGML.forEach(function(polygonGML) {
    // Get exterior and interior boundaries for polygon (outer and holes)
    var boundaries = citygmlBoundaries(polygonGML);

    // Get vertex points for the exterior boundary
    var points = citygmlPoints(boundaries.exterior[0]);

    allPolygons.push(points);
  });

  // Validate CityGML
  citygmlValidateShell(polygonsGML)
  .then((results) => repair(allPolygons, results))
  .then(function(results) {
    // Append data onto job payload
    _.extend(data, {
      polygons: results.polygons,
      flipFaces: results.flipFaces
    });

    queue.create('triangulate_building_queue', data).save(function() {
      done();
    });
  }).catch(function(err) {
    console.error(err);
    failBuilding(id, buildingId, done, err);
    return;
  });
};

var failBuilding = function(id, buildingId, done, err) {
  // Add building ID to failed buildings set
  redis.rpush('polygoncity:job:' + id + ':buildings_failed', JSON.stringify({id: buildingId, error: err.message})).then(function() {
    // Increment failed building count
    return redis.hincrby('polygoncity:job:' + id, 'buildings_count_failed', 1).then(function() {
      // Even though the model failed, don't pass on error otherwise job
      // will fail and prevent overall completion (due to a failed job)
      done();
    });
  });
};

var onExit = function() {
  console.log(chalk.red('Exiting repairBuilding worker...'));
  exiting = true;
};

process.on('SIGINT', onExit);

module.exports = worker;
