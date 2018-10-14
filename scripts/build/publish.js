'use strict';

// Node module dependencies
const fs = require('fs-extra-promise').useFs(require('fs-extra'));
const queue = require('queue');
const path = require('path');
const exec = require('child-process-promise').exec;
const branch = require('git-branch').sync();

const ROOT = path.resolve(path.join(__dirname, '../../'));
const DIST = path.resolve(ROOT, 'dist');
const MODULES = fs.readdirSync(DIST);
const FLAGS = ['--access public'];
const failedPackages = [];
const QUEUE = queue({
  concurrency: 10
});
const title = 'ClearRoad API Storages';

if (branch === 'develop') {
  FLAGS.unshift('--tag beta');
}

MODULES.forEach(module => {
  const PACKAGES = fs.readdirSync(path.resolve(DIST, module));

  PACKAGES.forEach(packageName => {
    QUEUE.push(done => {
      const NAME = `${module}/api-storage-${packageName}`;
      console.log(`Publishing ${NAME}`);

      const packagePath = path.resolve(DIST, module, packageName);
      const cmd = `npm publish ${packagePath} ${FLAGS.join(' ')}`;
      console.info(`--- Command: ${cmd}`);

      exec(cmd)
        .then(() => {
          console.log(`Done publishing ${NAME}!`);
          done();
        })
        .catch((e) => {
          console.error(e);
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
    console.log(`Done publishing ${title}!`);
  }
});
