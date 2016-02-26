var _ = require('lodash');
var Promise = require('bluebird');
var kue = require('kue');
var chalk = require('chalk');
var request = Promise.promisify(require('request'), {multiArgs: true});
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

var worker = function(job, done) {
  var data = job.data;
  var id = data.id;
  var buildingId = data.buildingId;

  var origin = data.originWGS84;

  var wofKey = (data.wofKey) ? data.wofKey : '';
  var url = data.wofEndpoint + '/?latitude=' + origin[1] + '&longitude=' + origin[0] + '&api_key=' + wofKey;

  // Retreive Who's on First results via API
  request(url).then(function(response) {
    var res = response[0];
    var body = response[1];

    if (res.statusCode != 200) {
      var err = new Error('Unexpected response, HTTP: ' + res.statusCode);
      console.error(err);
      console.log(body);
      failBuilding(id, buildingId, done, err);
      return;
    }

    try {
      var results = JSON.parse(body);

      if (results.length === 0) {
        var err = new Error('Who\'s on First results not present in API response');
        console.error(err);
        console.log(body);
        failBuilding(id, buildingId, done, err);
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
      console.log(body);
      failBuilding(id, buildingId, done, err);
      return;
    }
  }).catch(function(err) {
    if (err) {
      var err = new Error('Unable to retrieve Who\'s on First data' + ((err.message) ? ': ' + err.message : ''));
      console.error(err);
      failBuilding(id, buildingId, done, err);
      return;
    }
  });
};

var failBuilding = function(id, buildingId, done, err) {
  // Add building ID to failed buildings set
  redis.rpush('polygoncity:job:' + id + ':buildings_failed', JSON.stringify({id: buildingId, error: err.message})).then(function() {
    // Increment failed building count
    return redis.hincrby('polygoncity:job:' + id, 'buildings_count_failed', 1).then(function() {
      // Even though the model failed, don't pass on error otherwise job
      // will fail and prevent overall completion (due to a failed job)
      done();
    });
  });
};

var onExit = function() {
  console.log(chalk.red('Exiting whosOnFirst worker...'));
  exiting = true;
};

process.on('SIGINT', onExit);

module.exports = worker;
