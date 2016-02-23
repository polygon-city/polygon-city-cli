var kue = require('kue');
var chalk = require('chalk');
var worker = require(__dirname + '/../workers/whosOnFirst');

var redisHost = process.env.REDIS_PORT_6379_TCP_ADDR || '127.0.01';
var redisPort = process.env.REDIS_PORT_6379_TCP_PORT || 6379;

var queue = kue.createQueue({
  redis: {
    port: redisPort,
    host: redisHost,
  }
});

var onExit = function() {
  console.log(chalk.red('Exiting whosOnFirst queue...'));
};

process.on('SIGINT', onExit);

queue.process('whos_on_first_queue', function(job, done) {
  worker(job, done);
});
