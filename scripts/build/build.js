'use strict';

// Node module dependencies
const fs = require('fs-extra-promise').useFs(require('fs-extra'));
const queue = require('queue');
const path = require('path');
const exec = require('child_process').exec;
const config = require('../config.json');

// Constants for the build process. Paths and JSON files templates
const ROOT = path.resolve(path.join(__dirname, '../../')); // root actimirror-ionic directory
const STORAGES_PATH = path.resolve(ROOT, 'src', config.storageScope, 'storages'); // path to storages source files
const STORAGE_PACKAGE_JSON = require(path.resolve(__dirname, 'storage-package.json')); // storage package.json template
const STORAGE_TS_CONFIG = require(path.resolve(__dirname, 'tsconfig.storage.json')); // storage tsconfig template
const BUILD_TMP = path.resolve(ROOT, '.tmp'); // tmp directory path
const BUILD_DIST_ROOT = path.resolve(ROOT, config.storageDir); // dist directory root path

// Module version increment
let MODULE_PACKAGE_PATH = path.resolve(ROOT, 'package.json');
let MODULE_PACKAGE_JSON = require(MODULE_PACKAGE_PATH);
let MODULE_VERSION = MODULE_PACKAGE_JSON.version;

// Create tmp/dist directories
console.log('Making new TMP directory');
fs.mkdirpSync(BUILD_TMP);

// Fetch a list of the storages
const STORAGES = fs.readdirSync(STORAGES_PATH);

// Build specific list of storages to build from arguments, if any
let storagesToBuild = process.argv.slice(2),
    ignoreErrors = false,
    errors = [];

const index = storagesToBuild.indexOf('ignore-errors');
if (index > -1) {
  ignoreErrors = true;
  storagesToBuild.splice(index, 1);
  console.log('Build will continue even if errors were thrown. Errors will be printed when build finishes.');
}

if (!storagesToBuild.length) {
  storagesToBuild = STORAGES;
}

storagesToBuild.sort().reverse();

// Create a queue to process tasks
const QUEUE = queue({
  concurrency: Math.min(require('os').cpus().length, 2)
});

// Function to process a single storage
const addStorageToQueue = storageName => {
  QUEUE.push((callback) => {
    console.log(`Building storage: ${storageName}`);

    const STORAGE_BUILD_DIR = path.resolve(BUILD_TMP, 'storages', storageName);
    const STORAGE_SRC_PATH = path.resolve(STORAGES_PATH, storageName, 'index.ts');

    let tsConfigPath;

    fs.mkdirpAsync(STORAGE_BUILD_DIR) // create tmp build dir
      .then(() => fs.mkdirpAsync(path.resolve(BUILD_DIST_ROOT, storageName))) // create dist dir
      .then(() => {
        // Write tsconfig.json
        const tsConfig = JSON.parse(JSON.stringify(STORAGE_TS_CONFIG));
        tsConfig.files = [STORAGE_SRC_PATH];

        tsConfigPath = path.resolve(STORAGE_BUILD_DIR, 'tsconfig.json');

        return fs.writeJsonAsync(tsConfigPath, tsConfig);
      })
      .then(() => {
        // Write package.json
        const storagePackagePath = path.resolve(STORAGES_PATH, storageName, 'package.json');
        let packageJson = JSON.parse(JSON.stringify(STORAGE_PACKAGE_JSON));

        if (fs.existsSync(storagePackagePath)) {
          packageJson = require(storagePackagePath);
        }

        packageJson.name = `${config.storageScope}/api-storage-${storageName}`;
        packageJson.version = MODULE_VERSION;
        packageJson.description = packageJson.description.replace('{{STORAGE}}', storageName);
        packageJson.keywords.push(storageName);

        return fs.writeJsonAsync(path.resolve(BUILD_DIST_ROOT, storageName, 'package.json'), packageJson);
      })
      .then(() => {
        return fs.copyAsync(
          path.resolve(STORAGES_PATH, storageName, 'README.md'),
          path.resolve(BUILD_DIST_ROOT, storageName, 'README.md')
        );
      })
      .then(() => {
        // compile the storage
        exec(`${ROOT}/node_modules/.bin/tsc -p ${tsConfigPath}`, (err) => {
          if (err) {
            if (!ignoreErrors) {
              // oops! something went wrong.
              console.log(err);
              callback(`\n\nBuilding ${storageName} failed.`);
              return;
            }
            else {
              errors.push(err);
            }
          }
        });
      })
      .then(callback)
      .catch(callback);
  }); // QUEUE.push end
};

storagesToBuild.forEach(addStorageToQueue);

QUEUE.start((err) => {
  if (err) {
    console.log('Error building storages.');
    console.log(err);
    process.stderr.write(err);
    process.exit(1);
  }
  else if (errors.length) {
    errors.forEach(e => {
      console.log(e.message) && console.log('\n');
      process.stderr.write(err);
    });
    console.log('Build complete with errors');
    process.exit(1);
  }
  else {
    console.log('Done processing storages!');
  }
});
