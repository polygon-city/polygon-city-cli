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
  console.log('Mapzen key: %j', options.mapzen);
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
  if (!options.mapzen) {
    console.error(chalk.red('Exiting: Mapzen Elevation key not specified'));
    process.exit(1);
  }

  // Check that input file is valid
  checkFile(inputFile).then(function(inputPath) {
    console.log(chalk.green('Input file is accessible'));
    console.log(chalk.green(util.inspect(inputPath)));

    // Kick off processing job
    foreman.startJob(path.join(inputPath.dir, inputPath.base), path.normalize(options.output), options.epsg, options.mapzen);
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
  .option('-e, --epsg [code]', 'EPSG code for input data')
  .option('-m, --mapzen [key]', 'Mapzen Elevation API key')
  .option('-o, --output [directory]', 'Output directory')
  .action(processFile);

program
  .command('resume')
  .description('Resume processing of existing jobs')
  .action(resumeJobs);

program.parse(process.argv);
