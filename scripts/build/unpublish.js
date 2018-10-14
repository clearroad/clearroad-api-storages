'use strict';

// Node module dependencies
const fs = require('fs-extra-promise').useFs(require('fs-extra'));
const queue = require('queue');
const path = require('path');
const exec = require('child-process-promise').exec;

const ROOT = path.resolve(path.join(__dirname, '../../'));
const DIST = path.resolve(ROOT, 'dist');
const MODULES = fs.readdirSync(DIST);
const FLAGS = ['--access public', '--force'];
const failedPackages = [];
const QUEUE = queue({
  concurrency: 10
});
const title = 'ClearRoad API Storages';

MODULES.forEach(module => {
  const PACKAGES = fs.readdirSync(path.resolve(DIST, module));

  PACKAGES.forEach(packageName => {
    QUEUE.push(done => {
      const PACKAGE_JSON = require(path.resolve(DIST, module, packageName, 'package.json'));
      const NAME = PACKAGE_JSON.name;
      const VERSION = PACKAGE_JSON.version;

      console.log(`Unpublishing ${NAME}`);

      const cmd = `npm unpublish ${NAME}@${VERSION} ${FLAGS.join(' ')}`;
      console.info(`--- Command: ${cmd}`);

      exec(cmd)
        .then(() => {
          console.log(`Done unpublishing ${NAME}!`);
          done();
        })
        .catch((e) => {
          if (e.stderr && e.stderr.indexOf('previously published version') === -1) {
            failedPackages.push({
              cmd: e.cmd,
              stderr: e.stderr
            });
          }
          done();
        });
    });
  });
});

QUEUE.start((err) => {
  if (err) {
    console.error(`Error publishing ${title}`, err);
  }
  else if (failedPackages.length > 0) {
    console.error(`${failedPackages.length} packages failed to publish.`);
    console.error(failedPackages);
  }
  else {
    console.log(`Done unpublishing ${title}!`);
  }
});
