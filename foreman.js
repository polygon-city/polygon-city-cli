var kue = require('kue');
var Promise = require('bluebird');
var UUID = require('uuid');
var chalk = require('chalk');
var childProcess = require('child_process');
var request = Promise.promisify(require('request'), {multiArgs: true});
var Redis = require('ioredis');
var path = require('path');
var fs = require('fs-extra');

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

var queue = kue.createQueue({
  redis: {
    port: redisPort,
    host: redisHost,
  }
});

var streamBuildingsQueue;
var processes = [];

var exiting = false;

var setupQueues = function() {
  // Global handler to remove completed jobs
  queue.on('job complete', function(id, result){
    kue.Job.get(id, function(err, job){
      if (err) return;
      job.remove(function(err){
        if (err) throw err;
        // console.log('Removed completed job #%d', job.id);
      });
    });
  });

  var streamBuildingsWorker = require(__dirname + '/workers/streamBuildings');

  queue.process('stream_buildings_queue', function(job, done) {
    streamBuildingsWorker(job, done);
  });

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
  createQueue('whosOnFirst', 1);
  createQueue('buildingObj', 3);
  createQueue('convertObj', 4);
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

  startJob: function(options) {
    setupQueues();
    checkJobCompletion();

    console.log(options);

    // Generate unique ID for the file
    var id = UUID.v4();
    console.log('Start job:', id);

    // Add job to jobs list
    redis.rpush('polygoncity:jobs', id);

    // Add output path to job
    redis.hset('polygoncity:job:' + id, 'output_path', options.outputPath);

    getProj4Def(options.epsgCode).then(function(proj4def) {
      if (!proj4def) {
        var err = new Error('Unable to find Proj4 definition for EPSG code ' + epsgCode);
        console.error(chalk.red(err));
        onExit();
      }

      console.log('Proj4 definition:', proj4def);

      // Start everything going...
      queue.create('stream_buildings_queue', {
        id: id,
        prefix: options.prefix,
        inputPath: options.inputPath,
        outputPath: options.outputPath,
        epsgCode: options.epsgCode,
        proj4def: proj4def,
        mapzenKey: options.mapzenKey,
        elevationEndpoint: options.elevationEndpoint,
        wofEndpoint: options.wofEndpoint,
        attribution: options.attribution,
        license: options.license
      }).save();
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
          // Output failures
          redis.lrange('polygoncity:job:' + id + ':buildings_failed', 0, -1).then(function(failures) {
            failures.forEach(function(failure) {
              var failObj = JSON.parse(failure);
              console.error(chalk.yellow('Building ' + failObj.id + ' failed to process due to ' + failObj.error));
            });

            if (failures.length > 0) {
              redis.hget('polygoncity:job:' + id, 'output_path').then(function(outputPath) {
                var _outputPath = path.join(outputPath, 'failures.json');
                fs.outputFileSync(_outputPath, JSON.stringify(failures));
              });
            }

            redis.del('polygoncity:job:' + id);
            redis.del('polygoncity:job:' + id + ':buildings_failed');
            redis.lrem('polygoncity:jobs', 0, id);
          });
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

// process.once('SIGINT', function(sig) {
//   console.log(chalk.red('Exiting...'));
//   queue.shutdown(5000, function(err) {
//     console.log('Kue shutdown: ', err || '');
//     process.exit(0);
//   });
// });

var onExit = function(quitChildren) {
  console.log(chalk.red('Exiting...'));
  exiting = true;

  // Exit queues
  queue.shutdown(5000, function(err) {
    console.log('Kue shutdown');

    // Forcefully exit anything not already shut down
    processes.forEach(function(child) {
      child.kill('SIGKILL');
    });

    if (err) {
      console.error(chalk.red(err));
    }

    process.exit(1);
  });

  // if (quitChildren) {
  //   processes.forEach(function(child) {
  //     child.kill('SIGINT');
  //   });
  //
  //   setTimeout(function() {
  //     // Forcefully exit anything not already shut down
  //     processes.forEach(function(child) {
  //       child.kill('SIGKILL');
  //     });
  //
  //     process.exit(1);
  //   }, 5000);
  // }
};

process.once('SIGINT', onExit);

module.exports = foreman;
