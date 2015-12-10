var Queue = require('bull');
var worker = require('../workers/buildingObj');

var onQueueFailed = function(job, err) {
  console.error(chalk.red(err));
  process.exit(1);
};

var onQueueError = function(err) {
  console.error(chalk.red(err));
  process.exit(1);
};

var queue = Queue('building_obj_queue', 6379, '127.0.0.1');
queue.on('failed', onQueueFailed);
queue.on('error', onQueueError);
queue.process(worker);
