var _ = require('lodash');
var Promise = require('bluebird');
var kue = require('kue');
var chalk = require('chalk');
var request = Promise.promisify(require('request'), {multiArgs: true});

var redisHost = process.env.REDIS_PORT_6379_TCP_ADDR || '127.0.01';
var redisPort = process.env.REDIS_PORT_6379_TCP_PORT || 6379;

var queue = kue.createQueue({
  redis: {
    port: redisPort,
    host: redisHost,
  }
});

var exiting = false;

var worker = function(job, done) {
  var data = job.data;

  var origin = data.originWGS84;

  var url = data.wofEndpoint + '/?latitude=' + origin[1] + '&longitude=' + origin[0];

  // Retreive Who's on First results via API
  request(url).then(function(response) {
    var res = response[0];
    var body = response[1];

    if (res.statusCode != 200) {
      var err = new Error('Unexpected response, HTTP: ' + res.statusCode);
      console.error(err);
      done(err);
      return;
    }

    try {
      var results = JSON.parse(body);

      if (results.length === 0) {
        var err = new Error('Who\'s on First results not present in API response');
        console.error(err);
        done(err);
        return;
      }

      // Append data onto job payload
      _.extend(data, {
        wof: results
      });

      queue.create('building_obj_queue', data).save(function() {
        done();
      });
    } catch(err) {
      var err = new Error('Unexpected Who\'s on First data response' + ((err.message) ? ': ' + err.message : ''));
      console.error(err);
      done(err);
      return;
    }
  }).catch(function(err) {
    if (err) {
      var err = new Error('Unable to retrieve Who\'s on First data' + ((err.message) ? ': ' + err.message : ''));
      console.error(err);
      done(err);
      return;
    }
  });
};

var onExit = function() {
  console.log(chalk.red('Exiting whosOnFirst worker...'));
  exiting = true;
};

process.on('SIGINT', onExit);

module.exports = worker;
