#! /usr/bin/env node

var Promise = require('bluebird');
var program = require('commander');
var chalk = require('chalk');

var fs = Promise.promisifyAll(require('fs'));
var path = require('path');
var util = require('util');

var foreman = require('./foreman');

var checkFile = function(file) {
  var accessFile = fs.accessAsync(file);

  return accessFile.then(function() {
    return path.parse(file);
  }).catch(function(err) {
    console.error(chalk.red(err));
    throw err;
  });
};

var processFile = function(inputFile, options) {
  console.log('EPSG: %j', options.epsg);
  console.log('Elevation key: %j', options.elevationKey);

  if (options.prefix) {
    console.log('Prefix: %j', options.prefix);
  }

  if (options.elevationEndpoint) {
    console.log('Elevation endpoint: %j', options.elevationEndpoint);
  }

  if (options.wof) {
    console.log('Who\'s on First endpoint: %j', options.wof);
  }

  if (options.wofKey) {
    console.log('Who\'s on First key: %j', options.wofKey);
  }

  if (options.license) {
    console.log('License: %j', options.license);
  }

  console.log('Output directory: %j', program.output);
  console.log('Input: %j', inputFile);

  // Check input file path is defined
  if (!inputFile) {
    console.error(chalk.red('Exiting: Input file path not specified'));
    process.exit(1);
  }

  // Check output file path is defined
  if (!options.output) {
    console.error(chalk.red('Exiting: Output file path not specified'));
    process.exit(1);
  }

  // Check EPSG code
  if (!options.epsg) {
    console.error(chalk.red('Exiting: EPSG code not specified'));
    process.exit(1);
  }

  // Check Mapzen key
  if (!options.elevationKey) {
    console.error(chalk.red('Exiting: Mapzen Elevation key not specified'));
    process.exit(1);
  }

  // Check that input file is valid
  checkFile(inputFile).then(function(inputPath) {
    console.log(chalk.green('Input file is accessible'));
    console.log(chalk.green(util.inspect(inputPath)));

    // Kick off processing job
    foreman.startJob({
      inputPath: path.join(inputPath.dir, inputPath.base),
      outputPath: path.normalize(options.output),
      epsgCode: options.epsg,
      elevationKey: options.elevationKey,
      prefix: options.prefix,
      elevationEndpoint: options.elevation || 'https://elevation.mapzen.com',
      wofEndpoint: options.wof,
      wofKey: options.wofKey,
      attribution: options.attribution,
      license: options.license
    });
  }).catch(function(err) {
    console.error(chalk.red('Exiting:', err.message));
    process.exit(1);
  });
};

var resumeJobs = function() {
  foreman.resumeJobs();
};

program
  .version('0.0.1')
  .usage('[options] <input file>')
  .option('-c, --epsg [code]', 'EPSG code for input data')
  .option('-p, --prefix [prefix]', 'Prefix for building IDs')
  .option('-e, --elevation [url]', 'Elevation endpoint')
  .option('-E, --elevationKey [key]', 'Mapzen Elevation API key')
  .option('-w, --wof [url]', 'Who\'s On First endpoint')
  .option('-W, --wofKey [key]', 'Mapzen Who\'s on First API key')
  .option('-a, --attribution [attribution]', 'Attribution text')
  .option('-l, --license [license]', 'License text')
  .option('-o, --output [directory]', 'Output directory')
  .action(processFile);

program
  .command('resume')
  .description('Resume processing of existing jobs')
  .action(resumeJobs);

program.parse(process.argv);
