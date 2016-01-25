var _ = require('lodash');
var Promise = require('bluebird');
var Queue = require('bull');
var chalk = require('chalk');
var polygons2obj = require('polygons-to-obj');
var path = require('path');
var fs = Promise.promisifyAll(require('fs-extra'));

var redisHost = process.env.REDIS_PORT_6379_TCP_ADDR || '127.0.01';
var redisPort = process.env.REDIS_PORT_6379_TCP_PORT || 6379;

var convertObjQueue = Queue('convert_obj_queue', redisPort, redisHost);

var worker = function(job, done) {
  var data = job.data;

  var buildingId = data.buildingId;
  var polygons = data.polygons;
  var faces = data.faces;
  var origin = data.origin;
  var elevation = data.elevation;

  var outputPath = path.join(data.outputPath, "/models/", buildingId + '.obj');

  // Create OBJ using polygons and faces
  var objStr = polygons2obj(polygons, faces, origin, elevation, true);

  // Save OBJ file
  fs.outputFileAsync(outputPath, objStr).then(function() {
    console.log(chalk.green('Saved file:', outputPath));

    // Append data onto job payload
    _.extend(data, {
      objPath: outputPath
    });

    convertObjQueue.add(data).then(function() {
      done();
    });
  }).catch(function(err) {
    console.error(err);
    done(err);
  });
};

module.exports = worker;
