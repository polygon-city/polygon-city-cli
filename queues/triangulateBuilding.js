var Queue = require('bull');
var chalk = require('chalk');
var worker = require(__dirname + '/../workers/triangulateBuilding');

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

var queue = Queue('triangulate_building_queue', redisPort, redisHost);
queue.on('failed', onQueueFailed);
queue.on('error', onQueueError);
queue.on('completed', onCompleted);
queue.process(worker);
