// TODO: Quit after the entire queue has been cleared and converted
//
// Need to work out how to detect this

var Queue = require('bull');
var Promise = require('bluebird');
var UUID = require('uuid');
var chalk = require('chalk');
var childProcess = require('child_process');
var request = Promise.promisify(require('request'), {multiArgs: true});
var Redis = require('ioredis');

var onQueueFailed = function(job, err) {
  console.error(chalk.red(err));
  onExit();
};

var onQueueError = function(err) {
  console.error(chalk.red(err));
  onExit();
};

var onCompleted = function(job, data) {
  job.remove();
};

var redisHost = process.env.REDIS_PORT_6379_TCP_ADDR || '127.0.01';
var redisPort = process.env.REDIS_PORT_6379_TCP_PORT || 6379;

var redis = new Redis(redisPort, redisHost);

var streamBuildingsQueue;
var processes = [];

var exiting = false;

var setupQueues = function() {
  // Set stream queue up manually as otherwise the worker triggers an error
  streamBuildingsQueue = Queue('stream_buildings_queue', redisPort, redisHost);

  streamBuildingsQueue.on('failed', onQueueFailed);
  streamBuildingsQueue.on('completed', onCompleted);
  // Likely a problem connecting to Redis
  streamBuildingsQueue.on('error', onQueueError);

  var streamBuildingsWorker = require(__dirname + '/workers/streamBuildings');
  streamBuildingsQueue.process(streamBuildingsWorker);

  // Wrapper for spawning queues in separate processes
  //
  // TODO: Could probably have a single module for creating a queue, seeing as
  // they are all identical aside from the queue name
  var createQueue = function(name, workerCount) {
    var count = workerCount || 1;

    var child;
    while (count--) {
      child = childProcess.fork(__dirname + '/queues/' + name);
      processes.push(child);

      console.log(chalk.green('Spawned process ' + child.pid + ' for ' + name + ' from ' + process.pid));
    }
  };

  createQueue('repairBuilding', 3);
  createQueue('triangulateBuilding', 3);
  createQueue('buildingElevation', 5);
  createQueue('buildingObj', 3);
  createQueue('convertObj', 3);
  createQueue('geojsonIndex', 1);
};

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
  resumeJobs: function() {
    setupQueues();
    checkJobCompletion();
  },

  startJob: function(inputPath, outputPath, epsgCode, mapzenKey, prefix) {
    setupQueues();
    checkJobCompletion();

    // Generate unique ID for the file
    var id = UUID.v4();
    console.log('Start job:', id);

    // Add job to jobs list
    redis.rpush('polygoncity:jobs', id);

    getProj4Def(epsgCode).then(function(proj4def) {
      if (!proj4def) {
        var err = new Error('Unable to find Proj4 definition for EPSG code ' + epsgCode);
        console.error(chalk.red(err));
        onExit();
      }

      console.log('Proj4 definition:', proj4def);

      // Start everything going...
      streamBuildingsQueue.add({
        id: id,
        prefix: prefix,
        inputPath: inputPath,
        outputPath: outputPath,
        epsgCode: epsgCode,
        proj4def: proj4def,
        mapzenKey: mapzenKey
      });
    }).catch(function(err) {
      console.error(chalk.red(err));
      onExit();
    });
  },
};

var checkJobCompletion = function() {
  if (exiting) {
    return;
  }

  redis.lrange('polygoncity:jobs', 0, -1).then(function(result) {
    result.forEach(function(id) {
      redis.hget('polygoncity:job:' + id, 'completed').then(function(completed) {
        if (completed == 1) {
          redis.del('polygoncity:job:' + id);
          redis.lrem('polygoncity:jobs', 0, id);
        }
      });
    });

    redis.llen('polygoncity:jobs').then(function(count) {
      if (count == 0) {
        console.error(chalk.blue('Finished all jobs'));
        onExit(true);
      } else {
        setTimeout(checkJobCompletion, 1000);
      }
    });
  });
};

var onExit = function(quitChildren) {
  console.log(chalk.red('Exiting...'));
  exiting = true;

  if (quitChildren) {
    processes.forEach(function(child) {
      child.kill('SIGINT');
    });

    process.exit(1);
  }
};

process.on('SIGINT', onExit);

module.exports = foreman;
