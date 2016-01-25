var _ = require('lodash');
var Queue = require('bull');
var triangulate = require('triangulate');

var redisHost = process.env.REDIS_PORT_6379_TCP_ADDR || '127.0.01';
var redisPort = process.env.REDIS_PORT_6379_TCP_PORT || 6379;

var buildingElevationQueue = Queue('building_elevation_queue', redisPort, redisHost);

var worker = function(job, done) {
  var data = job.data;

  var polygons = data.polygons;
  var flipFaces = data.flipFaces;

  var allFaces = [];

  // TODO: Support polygons with holes
  polygons.forEach(function(polygon, pIndex) {
    // Triangulate faces
    try {
      var faces = triangulate(polygon);

      // Flip incorrect faces
      if (_.contains(flipFaces, pIndex)) {
        faces.forEach(function(face) {
          face.reverse();
        });
      }

      allFaces.push(faces);
    } catch (err) {
      done(err);
      return;
    }
  });

  // Append data onto job payload
  _.extend(data, {
    faces: allFaces
  });

  buildingElevationQueue.add(data).then(function() {
    done();
  });
};

module.exports = worker;
