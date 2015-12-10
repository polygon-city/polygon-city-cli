var Promise = require('bluebird');
var chalk = require('chalk');
var polygons2obj = require('polygons-to-obj');
var path = require('path');
var fs = Promise.promisifyAll(require('fs-extra'));

var worker = function(job, done) {
  var data = job.data;

  var buildingId = data.buildingId;
  var polygons = data.polygons;
  var faces = data.faces;
  var origin = data.origin;
  var elevation = data.elevation;

  var outputPath = path.join(data.outputPath, buildingId + '.obj');

  // Create OBJ using polygons and faces
  var objStr = polygons2obj(polygons, faces, origin, elevation, true);

  // Save OBJ file
  fs.outputFileAsync(outputPath, objStr).then(function() {
    console.log(chalk.green('Saved file:', outputPath));
    done();
  }).catch(function(err) {
    console.error(err);
    done(err);
  });
};

module.exports = worker;
