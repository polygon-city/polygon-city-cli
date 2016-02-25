var _ = require('lodash');
var Promise = require('bluebird');
var kue = require('kue');
var chalk = require('chalk');
var polygons2obj = require('polygons-to-obj');
var path = require('path');
var fs = Promise.promisifyAll(require('fs-extra'));
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
  var polygons = data.polygons;
  var faces = data.faces;
  var origin = data.origin;
  var elevation = data.elevation;

  var outputPath = path.join(data.outputPath, '/buildings/', buildingId, buildingId + '.obj');

  // Create OBJ using polygons and faces
  var objStr = polygons2obj(polygons, faces, origin, elevation, true);

  // Save OBJ file
  fs.outputFileAsync(outputPath, objStr).then(function() {
    console.log(chalk.green('Saved file:', outputPath));

    // Append data onto job payload
    _.extend(data, {
      objPath: outputPath
    });

    queue.create('convert_obj_queue', data).save(function() {
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
  console.log(chalk.red('Exiting buildingObj worker...'));
  exiting = true;
};

process.on('SIGINT', onExit);

module.exports = worker;
