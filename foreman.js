// TODO: Quit after the entire queue has been cleared and converted
//
// Need to work out how to detect this

var Queue = require('bull');
var Promise = require('bluebird');
var UUID = require('uuid');
var chalk = require('chalk');
var childProcess = require('child_process');
var request = Promise.promisify(require('request'), {multiArgs: true});

var onQueueFailed = function(job, err) {
  console.error(chalk.red(err));
  process.exit(1);
};

var onQueueError = function(err) {
  console.error(chalk.red(err));
  process.exit(1);
};

var onCompleted = function(job, data) {
  job.remove();
};

var redisHost = process.env.REDIS_PORT_6379_TCP_ADDR || '127.0.01';
var redisPort = process.env.REDIS_PORT_6379_TCP_PORT || 6379;

// Set stream queue up manually as otherwise the worker triggers an error
var streamBuildingsQueue = Queue('stream_buildings_queue', redisPort, redisHost);

streamBuildingsQueue.on('failed', onQueueFailed);
streamBuildingsQueue.on('completed', onCompleted);
// Likely a problem connecting to Redis
streamBuildingsQueue.on('error', onQueueError);

var streamBuildingsWorker = require(__dirname + '/workers/streamBuildings');
streamBuildingsQueue.process(streamBuildingsWorker);

var processes = [];

// Wrapper for spawning queues in separate processes
//
// TODO: Could probably have a single module for creating a queue, seeing as
// they are all identical aside from the queue name
var createQueue = function(name, workerCount) {
  var count = workerCount || 1;

  while (count--) {
    processes.push(childProcess.fork(__dirname + '/queues/' + name));
  }
};

createQueue('repairBuilding', 3);
createQueue('triangulateBuilding', 3);
createQueue('buildingElevation', 5);
createQueue('buildingObj', 3);
createQueue('convertObj', 3);

var getProj4Def = function(epsgCode) {
  return request('http://epsg.io/?q=' + epsgCode + '&format=json').then(function(response) {
    var res = response[0];
    var body = response[1];

    if (res.statusCode != 200) {
      var err = new Error('Unexpected epsg.io response, HTTP: ' + res.statusCode);
      return Promise.reject(err);
    }

    try {
      var bodyJSON = JSON.parse(body);
      return Promise.resolve(bodyJSON.results[0].proj4);
    } catch(err) {
      var err = new Error('Unexpected epsg.io response' + ((err.message) ? ': ' + err.message : ''));
      return Promise.reject(err);
    }
  }).catch(function(err) {
    return Promise.reject(err);
  });
};

var foreman = {
  startJob: function(inputPath, outputPath, epsgCode, mapzenKey) {
    // Generate unique ID for the file
    var id = UUID.v4();
    console.log('Start job:', id);

    getProj4Def(epsgCode).then(function(proj4def) {
      if (!proj4def) {
        var err = new Error('Unable to find Proj4 definition for EPSG code ' + epsgCode);
        console.error(chalk.red(err));
        process.exit(1);
      }

      console.log('Proj4 definition:', proj4def);

      // Start everything going...
      streamBuildingsQueue.add({
        id: id,
        inputPath: inputPath,
        outputPath: outputPath,
        epsgCode: epsgCode,
        proj4def: proj4def,
        mapzenKey: mapzenKey
      });
    }).catch(function(err) {
      console.error(chalk.red(err));
      process.exit(1);
    });
  },
};

var onExit = function() {
  processes.forEach(function(child) {
    child.kill();
  });

  process.exit(1);
};

process.on('exit', onExit);
process.on('SIGINT', onExit);

module.exports = foreman;
