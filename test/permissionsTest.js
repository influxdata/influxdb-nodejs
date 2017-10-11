/* global describe it before beforeEach */
/**
 * Created by karl on 4.8.17.
 */
import * as linereader from 'readline';
import * as fs from 'fs';
import assert from 'assert';
import * as InfluxDB from '~/InfluxDB';
import * as utils from './utils/utils.js';
import * as InfluxDBServer from '~/../scripts/InfluxDBServer.js';

describe('Permissions test', () => {
    // set up users and databases

  function processQueries(queries, cxn) {
    let index = 0;

    // need to execute lines from ql script pseudo synchronously
    return new Promise((resolve, reject) => {
      function next() {
        if (index < queries.length) {
          console.log(`processing ${queries[index]}`);
          cxn.executeQuery(queries[index++]).then(next, reject);
        } else {
          resolve();
        }
      }
      next();
    });
  }

  function setupDatabase() {
    // Next setup the users and databases
    const result = new Promise((resolve, reject) => {
      const cxnSetup = new InfluxDB.Connection({
        database: '_internal',
        username: 'admin',
        password: 'noname',
        autoCreateDatabase: false,
      });

      console.log(JSON.stringify(cxnSetup.stub.options));

      cxnSetup.connect()
      .then(() => {
        const lr = linereader.createInterface({
          input: fs.createReadStream(`${__dirname}/../etc/testdbs-setup.inql`),
        });
        const queries = [];
        lr.on('line', (line) => {
          const doline = line.trim();
          if (doline.length && doline[0] !== '-') {
            queries.push(line.trim());
          }
        });
        lr.on('close', () => {
          processQueries(queries, cxnSetup);
          resolve('Querries processed');
        });
      })
      .catch((e) => {
        console.log(e);
        reject(e);
      });
    });
    return result;
  }

  before(function (done) {
    // First restart the docker server
    this.timeout(180000);
    if (!utils.testconf.dynamic) {
      console.log('Using static system server.  Cannot redeploy new influxdb server. Skipping');
      this.skip();
 //     done();
    } else {
      const args = `http --version ${utils.testconf.influxdb.release} --admin admin --password noname`;

      console.log(`Using dynamic servers.  Reconfiguring and restarting influxdb server ${args}`);
//      utils.startDockerInfluxdb(args)
      InfluxDBServer.startDocker(args)
        .catch((e) => { done(e); })
        .then(() => {
          console.log('Sleeping for 45 sec', new Date().getTime());
          utils.sleep(45000)
            .then(() => {
              console.log('waking after 45 sec', new Date().getTime());
              setupDatabase()
                .then(() => {
                  utils.sleep(2000)
                    .then(() => {
                      console.log('exiting before all successfully');
                      done();
                    });
                })
                .catch((e) => {
                  done(e);
                });
            });
        });
    }
  });

  beforeEach(function () {
    if (!utils.testconf.dynamic) {
      console.log('Using static system server.  Cannot redeploy new influxdb server. Skipping');
      this.skip();
      // done();
    }
  });

  describe('Work with restricted db as user granted all', () => {
    const cxnotto = new InfluxDB.Connection({
      username: 'otto',
      password: 'noname',
      database: 'ecurrentsdb',
      autoCreateDatabase: false,
    });

   // console.log(JSON.stringify(cxnotto));


    const testdps = utils.buildDatapoints('voltage',
      [{ name: 'location', base: 'substation', type: 'string' }],
      [{ name: 'v2consumer', base: 200, type: 'float' }],
      100);


    it('#user granted all should write datapoints', (done) => {
      // console.log(JSON.stringify(testdps));
   //   this.timeout(14000);
     // setTimeout(() => {
      cxnotto.connect()
        .then(() => cxnotto.write(testdps))
        .then(() => cxnotto.flush())
        .then(done, done)
/*        .then(() => {
          done();
        }) */
        .catch((e) => {
          done(e);
        });
     // }, 10000);
    });

    it('#user granted all should readback the datapoints', (done) => {
      cxnotto.connect()
        .then(() => cxnotto.executeQuery('Select * from voltage'))
        .then((result) => {
//          console.log(`Query result ${JSON.stringify(result)}`);
          assert(result.length === testdps.length);
        })
        .then(done, done)
        .catch((e) => {
          done(e);
        });
    });
  });
});
