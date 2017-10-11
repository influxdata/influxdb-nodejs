/* global describe it before after */
import assert from 'assert';
import * as InfluxDB from '~/InfluxDB';
import * as utils from '~/../test/utils/utils';


describe('HTTPS server test', () => {
  let failed = false;
  before(function (done) {
    this.timeout(90000);
    if (!utils.testconf.dynamic) {
      console.log('Using static system server.  Cannot redeploy new influxdb server. Skipping');
      this.skip();
      done();
    } else {
      const args = `https --version=${utils.testconf.influxdb.release}`;

      console.log(`Using dynamic servers.  Reconfiguring and restarting influxdb server ${args}`);
      utils.startDockerInfluxdb(args);
      console.log('waiting 1 min for server to start');
      utils.sleep(60000).then(() => {
        done();
      });
    }
  });

  after(function (done) {
    this.timeout(90000);
    // restore simple http server
    if (!utils.testconf.dynamic) {
      console.log('Using static system server.  No need to reset influxdb server');
      done();
    } else {
      if (failed) {
        console.log('Test failed.  Preserving server for investigation');
        done();
      }
      const args = `http --version=${utils.testconf.influxdb.release}`;
      console.log(`Using dynamic servers.  Restoring simple influxdb server ${args}`);
      utils.startDockerInfluxdb(args);
      console.log('waiting 1 min for server to start');
      utils.sleep(60000).then(() => {
        done();
      });
      done();
    }
  });

  const connection = new InfluxDB.Connection({

    database: 'test1',
    hostUrl: 'https://localhost:8086',

  });

  const dps = utils.buildDatapoints('wetbulb',
    [{ name: 'locale', base: 'a', type: 'string' }],
    [{ name: 'celsius', base: 18, type: 'float' }],
    30);

  process.env.NODE_TLS_REJECT_UNAUTHORIZED = '0';

  it('#should write and read datapoints over https', (done) => {
    connection.connect()
      .then(() => connection.write(dps))
      .then(() => connection.flush())
      .then(() => connection.executeQuery('select * from wetbulb'))
      .then((result) => {
        try {
          assert(result.length, dps.length);
          done(utils.dropMeasurement(connection, 'wetbulb'));
        } catch (e) {
          utils.dropMeasurement(connection, 'wetbulb');
          done(e);
        }
      })
      .catch((e) => {
        failed = true;
        done(e);
      });
  });
});
