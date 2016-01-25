var _ = require('lodash');
var Promise = require('bluebird');
var Queue = require('bull');
var chalk = require('chalk');
var path = require('path');
var modelConverter = require('model-converter');

var redisHost = process.env.REDIS_PORT_6379_TCP_ADDR || '127.0.01';
var redisPort = process.env.REDIS_PORT_6379_TCP_PORT || 6379;

var geojsonIndexQueue = Queue('geojson_index_queue', redisPort, redisHost);

var exiting = false;

var convertQueue = function(input, outputs) {
  var promises = [];

  for (var i = 0; i < outputs.length; i++) {
    promises.push(modelConverter.convert(input, outputs[i]));
  }

  return Promise.all(promises);
};

var worker = function(job, done) {
  var data = job.data;

  var id = data.id;
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

      // Append data onto job payload
      _.extend(data, {
        convertedPaths: outputPaths
      });

      geojsonIndexQueue.add(data).then(function() {
        done();
      });
    })
    .catch(function(err) {
      console.error(err);
      done(err);
    });
};

var onExit = function() {
  console.log(chalk.red('Exiting convertObj worker...'));
  exiting = true;
  // process.exit(1);
};

process.on('SIGINT', onExit);

module.exports = worker;
