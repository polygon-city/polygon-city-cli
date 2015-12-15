var Promise = require('bluebird');
var chalk = require('chalk');
var path = require('path');
var modelConverter = require('model-converter');

var convertQueue = function(input, outputs) {
  var promises = [];

  for (var i = 0; i < outputs.length; i++) {
    promises.push(modelConverter.convert(input, outputs[i]));
  }

  return Promise.all(promises);
};

var worker = function(job, done) {
  var data = job.data;

  var objPath = data.objPath;

  // Break path into components
  var pathComponents = path.parse(objPath);

  var input = objPath;

  // First convert as obj to clean things up
  var output = path.join(pathComponents.dir, pathComponents.name + ".obj");

  // Queue up conversions to other formats
  var outputs = [];
  outputs.push(path.join(pathComponents.dir, pathComponents.name + ".dae"));
  // outputs.push(path.join(pathComponents.dir, pathComponents.name + ".ply"));
  // // outputs.push(path.join(pathComponents.dir, pathComponents.name + ".gltf"));

  // First convert as obj to clean things up
  modelConverter.convert(input, output)
    .then(function(outputPath) {
      console.log('Conversion input:', input);
      console.log('Conversion output:', outputPath);

      return convertQueue(input, outputs);
    })
    .then(function(outputPaths) {
      console.log('Conversion input:', input);
      console.log('Conversion outputs:', outputPaths);

      done();
    })
    .catch(function(err) {
      console.error(err);
      done(err);
    });
};

module.exports = worker;