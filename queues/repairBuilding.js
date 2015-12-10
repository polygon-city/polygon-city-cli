var Queue = require('bull');
var worker = require('../workers/repairBuilding');

var onQueueFailed = function(job, err) {
  console.error(chalk.red(err));
  process.exit(1);
};

var onQueueError = function(err) {
  console.error(chalk.red(err));
  process.exit(1);
};

var queue = Queue('repair_building_queue', 6379, '127.0.0.1');
queue.on('failed', onQueueFailed);
queue.on('error', onQueueError);
queue.process(worker);
