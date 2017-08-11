const cproc = require('child_process');
const utils = require('../test/utils/utils.js');

let dockerProcess;
  /**
   * @param args - array of arguments to the python script
   *    valid arguments:
   *       * http || https
   *       * --version VERSION version of influxdb to pull - default 'latest'
   *       * --admin ADMIN admin user name
   *       * --password PASSWORD admin password
   *       * --nopull NOPULL do not pull new image
   *       * --name NAME of the container - default 'influxdb'
   */
function startDocker(args) {
    // const pyArgs = [`${__dirname}/../scripts/test-server.py`, args];

  console.log(`(re)starting influxbd docker container ${__dirname}/../scripts/test-server.py ${args}`);

  const result = new Promise((resolve, reject) => {
      // dockerProcess = exec('python3', pyArgs)
  //    dockerProcess = exec(`${__dirname}/../scripts/test-server.py ${args}`)
    dockerProcess = cproc.exec(`${__dirname}/test-server.py ${args}`);

    let stdout = '';
    dockerProcess.stdout.on('data', (data) => {
      stdout += data.toString();
    });

    dockerProcess.on('close', (code) => {
      console.log('docker process closed ', stdout);
      if (code !== 0) {
        reject(new Error(`Child processes ended with ${code}`));
      }

        // wait a 30 secs for server to start
      console.log('Waiting 30 seconds for influxdb server to start');
      utils.sleep(30000).then(() => {
          // console.log('STDOUT ' + stdout)
        console.log(`started influxdb server with args ${args}`);
        resolve('restarted influxdb');
      });
    });
  });
  return result;
}

module.exports = { startDocker };
