var Queue = require('bull');
var worker = require('../workers/buildingElevation');

var onQueueFailed = function(job, err) {
  console.error(chalk.red(err));
  process.exit(1);
};

var onQueueError = function(err) {
  console.error(chalk.red(err));
  process.exit(1);
};

var queue = Queue('building_elevation_queue', 6379, '127.0.0.1');
queue.on('failed', onQueueFailed);
queue.on('error', onQueueError);
queue.process(worker);
