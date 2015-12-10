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

program
  .version('0.0.1')
  .usage('[options] <input file>')
  .option('-e, --epsg [code]', 'EPSG code for input data')
  .option('-m, --mapzen [key]', 'Mapzen Elevation API key')
  .option('-o, --output [directory]', 'Output directory')
  .parse(process.argv);

var args = program.args;

console.log('EPSG: %j', program.epsg);
console.log('Mapzen key: %j', program.mapzen);
console.log('Output directory: %j', program.output);
console.log('Input: %j', args[0]);

// Check input file path is defined
if (!args[0]) {
  console.error(chalk.red('Exiting: Input file path not specified'));
  process.exit(1);
}

// Check output file path is defined
if (!program.output) {
  console.error(chalk.red('Exiting: Output file path not specified'));
  process.exit(1);
}

// Check EPSG code
if (!program.epsg) {
  console.error(chalk.red('Exiting: EPSG code not specified'));
  process.exit(1);
}

// Check Mapzen key
if (!program.mapzen) {
  console.error(chalk.red('Exiting: Mapzen Elevation key not specified'));
  process.exit(1);
}

// Check that input file is valid
checkFile(args[0]).then(function(inputPath) {
  console.log(chalk.green('Input file is accessible'));
  console.log(chalk.green(util.inspect(inputPath)));

  // Kick off processing job
  foreman.startJob(path.join(inputPath.dir, inputPath.base), path.normalize(program.output), program.epsg, program.mapzen);
}).catch(function(err) {
  console.error(chalk.red('Exiting:', err.message));
  process.exit(1);
});
