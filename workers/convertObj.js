var _ = require('lodash');
var Promise = require('bluebird');
var kue = require('kue');
var chalk = require('chalk');
var path = require('path');
var fs = require('fs');
var modelConverter = require('model-converter');
var JXON = require('jxon');
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
var activeJob = false;

var convertQueue = function(input, outputs) {
  var promises = [];

  for (var i = 0; i < outputs.length; i++) {
    promises.push(modelConverter.convert(input, outputs[i]));
  }

  return Promise.all(promises);
};

var worker = function(job, done) {
  if (exiting) {
    return;
  }

  activeJob = true;

  var data = job.data;

  var id = data.id;
  var buildingId = data.buildingId;
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

      var allPaths = [].concat(input, outputPaths);

      // Add metadata to building model files (origin, etc)

      var metadata = {
        id: data.buildingId,
        idOriginal: data.buildingIdOriginal,
        origin: data.originWGS84,
        elevation: data.elevation,
      };

      if (data.wof) {
        metadata.wof = data.wof;
      }

      var jsonStr = '# ' + JSON.stringify(metadata) + '\n';

      var licenseObjStr = '';
      if (data.attribution) {
        licenseObjStr += '# Attribution: ' + data.attribution + '\n';
      }

      if (data.license) {
        licenseObjStr += '# License: ' + data.license + '\n';
      }

      var originObjStr = '# Longitude: ' + data.originWGS84[0] + '\n# Latitude: ' + data.originWGS84[1] + '\n';
      var elevationObjStr = '# Elevation: ' + data.elevation + '\n\n';

      var modelData, newModelData, jxonObj;
      allPaths.forEach(function(outputPath) {
        // Open each model file
        modelData = fs.readFileSync(outputPath);

        newModelData = undefined;

        if (outputPath.endsWith('.obj')) {
          // If obj, inject origin as comment at top
          newModelData = jsonStr + licenseObjStr + originObjStr + elevationObjStr + modelData.toString();
        } else if (outputPath.endsWith('.dae')) {
          // If collada, convert to XML and inject origin somewhere sane
          jxonObj = JXON.stringToJs(modelData.toString());

          jxonObj.collada.asset.longitude = data.originWGS84[0];
          jxonObj.collada.asset.latitude = data.originWGS84[1];
          jxonObj.collada.asset.elevation = data.elevation;

          if (data.attribution) {
            jxonObj.collada.asset.attribution = data.attribution;
          }

          if (data.license) {
            jxonObj.collada.asset.license = data.license;
          }

          jxonObj.collada.asset.metadata = JSON.stringify(metadata);

          newModelData = JXON.jsToString(jxonObj);
        }

        if (!newModelData) {
          return;
        }

        // Save model file
        fs.writeFileSync(outputPath, newModelData);
      });

      // Append data onto job payload
      _.extend(data, {
        convertedPaths: outputPaths
      });

      queue.create('geojson_index_queue', data).save(function() {
        activeJob = false;
        done();
      });
    })
    .catch(function(err) {
      console.error(err);
      activeJob = false;
      failBuilding(id, buildingId, done, err);
      return;
    });
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
  console.log(chalk.red('Exiting convertObj worker...'));
  exiting = true;

  // Keep process active until job is complete
  exitAfterJob();
};

var exitAfterJob = function() {
  if (!activeJob) {
    return;
  } else {
    setTimeout(exitAfterJob, 500);
  }
};

process.on('SIGINT', onExit);

module.exports = worker;
