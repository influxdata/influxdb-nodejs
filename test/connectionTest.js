/* global describe it */
import assert from 'assert';
import * as _ from 'lodash';

import * as InfluxDB from '~/InfluxDB';
import * as util from '~/../scripts/utils';

describe('Connection test', () => {
  describe('Automatic connect and write', () => {
    const connection = new InfluxDB.Connection({
      database: 'test1',
    });

    const dataPoint1 = {
      measurement: 'location',
      tags: [{ key: 'unit', value: 'adam-12' }],
      fields: [{ key: 'coordinates', value: '34_06_46_N_118_20_20_W' }],
    };

    const dataPoint2 = {
      measurement: 'location',
      tags: [{ key: 'unit', value: 'l-30' }],
      fields: [{ key: 'coordinates', value: '34.11856_N_118.30037_W' }],
    };

    const dataPoint3 = {
      measurement: 'location',
      tags: [{ key: 'unit', value: 'zebra-07' }],
      fields: [{ key: 'coordinates', value: '33_56_33_N_118_24_29_W' }],
    };

    it('should automatically connect and write data points, incl. timestamp', () => {
      connection.write([dataPoint1, dataPoint2, dataPoint3], true);
    });

    it('should read back and verify the data', (done) => {
      util.sleep(500)
        .then(() => connection.executeQuery('Select * from location'))
        .then((result) => {
          assert.equal(result.length, 3);
          result.forEach((dataPoint) => {
            assert(dataPoint.time !== undefined);
            switch (dataPoint.unit) {
              case 'adam-12':
                assert.equal(dataPoint.coordinates, dataPoint1.fields[0].value);
                break;
              case 'l-30':
                assert.equal(dataPoint.coordinates, dataPoint2.fields[0].value);
                break;
              case 'zebra-07':
                assert.equal(dataPoint.coordinates, dataPoint3.fields[0].value);
                break;
              default:
                assert.fail(dataPoint.unit, dataPoint.coordinates, 'Unexpected element in results array');
                break;
            }
          });
          done();
        }, done);
    });

    it('should drop the measurement', (done) => {
      const result = util.dropMeasurement(connection, 'location');
      // take a moment for transaction to complete otherwise lose connection object too soon in
      // some cases
      util.sleep(1000).then(() => {
        done(result);
      });
    });
  });

  describe('Buffer size triggered automatic write', () => {
    const connection = new InfluxDB.Connection({
      database: 'test1',
      batchSize: 100,
      maximumWriteDelay: 5000, // set longer write delay, ensure auto write triggered by buffer size
    });

    const testDataPoints = util.buildDatapoints('distance',
      [{ name: 'unit', base: 'baker-', type: 'string' }],
      [{ name: 'frombase', base: 0, type: 'float' }],
      300); // force buffer to flush three times in succession
    const chunkSize = 10;
    // will write in chunks of 10 items
    const dataPointChunks = testDataPoints
      .map((e, i) => (i % chunkSize === 0 ? testDataPoints.slice(i, i + chunkSize) : null))
      .filter(e => e);

    it('should write successive chunks to buffer and trigger write to db', (done) => {
      connection.connect().then(() => {
        dataPointChunks.forEach((chunk) => {
          connection.write(chunk).catch((e) => {
            done(e);
          });
        });
        done();
      }, done);
    });

    it('should read back the data', (done) => {
      connection.executeQuery('SELECT * FROM distance')
        .then((result) => {
          assert(result.length, testDataPoints.length);
          done();
        }, done);
    });

    it('should drop the test measurement', (done) => {
      const result = util.dropMeasurement(connection, 'distance');
      // take a moment for transaction to complete
      // otherwise lose connection object too soon in some cases
      util.sleep(1000).then(() => {
        done(result);
      });
    });
  });

  describe('Timeout triggered automatic write', () => {
    const writeDelay = 1000; // default writeDelay
    const connection = new InfluxDB.Connection({
      database: 'test1',
      maximumWriteDelay: writeDelay,
    });

    const dataPoint1 = {
      measurement: 'temperature',
      timestamp: new Date(),
      tags: [{ key: 'turbine', value: 'bremerhaven-0013' }],
      fields: [{ key: 'celsius', value: '67.3' }],
    };

    const dataPoint2 = {
      measurement: 'temperature',
      timestamp: new Date(),
      tags: [{ key: 'turbine', value: 'bremerhaven-0017' }],
      fields: [{ key: 'celsius', value: '22' }],
    };

    const dataPoint3 = {
      measurement: 'temperature',
      timestamp: new Date(),
      tags: [{ key: 'turbine', value: 'bremerhaven-0019' }],
      fields: [{ key: 'celsius', value: '39.5' }],
    };

    it('should write to db after delay expires', (done) => {
      connection.connect().then(() =>
        connection.write([dataPoint1, dataPoint2, dataPoint3])).then(done, done);
    });

    it(`should read back the data after the delay of ${writeDelay}ms expires`, (done) => {
      connection.connect()
        .then(() => util.sleep(writeDelay))
        .then(() => connection.executeQuery('Select * from temperature'))
        .then((result) => {
          assert.equal(result.length, 3);
          result.forEach((dataPoint) => {
            switch (dataPoint.turbine) {
              case 'bremerhaven-0013':
                assert.equal(dataPoint.celsius, dataPoint1.fields[0].value);
                break;
              case 'bremerhaven-0017':
                assert.equal(dataPoint.celsius, dataPoint2.fields[0].value);
                break;
              case 'bremerhaven-0019':
                assert.equal(dataPoint.celsius, dataPoint3.fields[0].value);
                break;
              default:
                throw new Error('Unknown element in results array');
            }
          });
          done();
        }, done);
    });

    it('should drop the test data', (done) => {
      const result = util.dropMeasurement(connection, 'temperature');
      util.sleep(1000).then(() => {
        done(result);
      });
    });
  });

  describe('autoResolveBufferedWritePromises - false', () => {
    const autoResolveWritesConnection = new InfluxDB.Connection({
      database: 'test1',
      autoResolveBufferedWritePromises: true,
      maximumWriteDelay: 1000,
    });

    const noAutoResolveWritesConnection = new InfluxDB.Connection({
      database: 'test1',
      autoResolveBufferedWritePromises: false,
      maximumWriteDelay: 1000,
    });


    const dataPoints = util.buildDatapoints('temp',
      [{ name: 'thermometer', base: 'tmeter', type: 'string' }],
      [{ name: 'cels', base: 17, type: 'float' }],
      30);

    let autoWriteTime = 0;
    let start;

    // get initial time of autoResolveBufferedWritePromises: true for later comparison
    autoResolveWritesConnection.connect()
      .then(() => {
        start = new Date().getTime();
        return autoResolveWritesConnection.write(dataPoints);
      })
      .then(() => {
        const end = new Date().getTime();
        autoWriteTime = end - start;
      })
      .catch((e) => {
        console.log('error', e);
      });

    util.dropMeasurement(autoResolveWritesConnection, 'temp');
    // take a moment for transaction to complete
    // otherwise lose connection object too soon in some cases
    util.sleep(2000).then(() => {
    });

    it('writes should be resolved after corresponding data are written into db', (done) => {
      let waitWriteTime = 0;
      let waitStart;
      noAutoResolveWritesConnection.connect()
        .then(() => {
          waitStart = new Date().getTime();
          return noAutoResolveWritesConnection.write(dataPoints);
        })
        .then(() => {
          const end = new Date().getTime();
          waitWriteTime = end - waitStart;
          console.log(`test ${util.pad(31, 10, '0')}`);
          console.log(`autoResolveBufferedWritePromises(false) ${util.pad(waitWriteTime, 6, ' ')}ms`);
          console.log(`autoResolveBufferedWritePromises(true)  ${util.pad(autoWriteTime, 6, ' ')}ms`);
          console.log(`autoResolveBufferedWritePromises(diff)  ${util.pad(waitWriteTime - autoWriteTime, 6, ' ')}ms`);
          assert(waitWriteTime > autoWriteTime);
          assert(waitWriteTime > autoWriteTime);
          assert(waitWriteTime > 1000);
          done();
        }, done);
    });

    it('should read the points back', (done) => {
      noAutoResolveWritesConnection.connect()
        .then(() => noAutoResolveWritesConnection.executeQuery('select * from temp'))
        .then((result) => {
          assert(result.length === dataPoints.length);
          done();
        }, done);
    });

    it('should drop the datapoints', (done) => {
      const result = util.dropMeasurement(noAutoResolveWritesConnection, 'temp');
      // take a moment for transaction to complete
      // otherwise lose connection object too soon in some cases
      util.sleep(1000).then(() => {
        done(result);
      });
    });
  });


  describe('autoCreateDatabase: false', () => {
    const existingDB = 'reified';
    const nonExistingDB = 'phantomdb';

    const connectionToNonExistingDB = new InfluxDB.Connection({
      database: nonExistingDB,
      autoCreateDatabase: false,
    });

    const autoCreateDB = new InfluxDB.Connection({
      database: existingDB,
      autoCreateDatabase: true,
    });

    const noAutoCreateDB = new InfluxDB.Connection({
      database: existingDB,
      autoCreateDatabase: false,
    });

    const testDataPoint = {
      measurement: 'flips',
      timestamp: new Date(),
      tags: [{ key: 'turbine', value: 'bremerhaven-0019' }],
      fields: [{ key: 'flops', value: '99' }],
    };

    it('should set up a preexisting database', (done) => {
      autoCreateDB.connect().then(done, done);
    });

    it('should fail to write to a non-existent database', (done) => {
      connectionToNonExistingDB.connect()
        .then(() => connectionToNonExistingDB.write([testDataPoint]))
        .then(() => connectionToNonExistingDB.flush())
        .then(() => connectionToNonExistingDB.executeQuery('SHOW DATABASES'))
        .then((result) => {
          done(new Error(`No error on write to the nonexistant database ${nonExistingDB}. Current Databases are ${JSON.stringify(result)}`));
        })
        .catch((e) => {
          console.log(e);
          done();
        });
    });

    it('should write to an existing database', (done) => {
      noAutoCreateDB.connect()
        .then(() => noAutoCreateDB.write([testDataPoint], true))
        .then(done, done);
    });

    it('should read back the data from the existing database', (done) => {
      util.sleep(500)
        .then(() => noAutoCreateDB.connect())
        .then(() => noAutoCreateDB.executeQuery('select * from flips'))
        .then((result) => {
          try {
            assert(result.length > 0);
            done();
          } catch (e) {
            done(e);
          }
        }, done);
    });

    it('should drop the database just created', (done) => {
      // wait to make sure other operations have completed
      util.sleep(1000)
        .then(() => autoCreateDB.connect())
        .then(() => autoCreateDB.executeQuery(`DROP DATABASE ${existingDB}`)).then(() => {
          done();
        }, done);
    });
  });

  describe('Check batching behaviour', () => {
    it('should automatically write when batch size is 0', (done) => {
      const connection = new InfluxDB.Connection({
        database: 'test2',
        batchSize: 0,
      });

      const testDataPoint = {
        measurement: 'flips',
        timestamp: new Date(),
        tags: [{ key: 'turbine', value: 'bremerhaven-0019' }],
        fields: [{ key: 'flops', value: '99' }],
      };

      connection.connect()
        .then(() => connection.write([testDataPoint]))
        .then(() => util.sleep(500))
        .then(() => connection.executeQuery('SELECT * FROM flips'))
        .then((result) => {
          try {
            assert(result.length > 0);
            done(util.dropMeasurement(connection, 'flips'));
          } catch (e) {
            done(e);
          }
        }, done);
    });

    it('should automatically write when minimumWriteDelay is 0', (done) => {
      const connection = new InfluxDB.Connection({
        database: 'test2',
        maximumWriteDelay: 0,
      });

      const testDataPoint = {
        measurement: 'flops',
        timestamp: new Date(),
        tags: [{ key: 'turbine', value: 'bremerhaven-0019' }],
        fields: [{ key: 'flips', value: '99' }],
      };

      connection.connect()
        .then(() => connection.write([testDataPoint]))
        .then(() => util.sleep(500))
        .then(() => connection.executeQuery('SELECT * FROM flops'))
        .then((result) => {
          try {
            assert(result.length > 0);
            done(util.dropMeasurement(connection, 'flops'));
          } catch (e) {
            done(e);
          }
        }, done);
    });


    it('should not write measurements to DB before a proper delay expires', (done) => {
      const connection = new InfluxDB.Connection({
        database: 'test1',
      });
      connection.connect().then(() => {
        const onEmptyMeasurement = function () {
          const dataPoint1 = {
            measurement: 'outdoorThermometerA',
            timestamp: new Date(),
            tags: {
              location: 'greenhouse',
            },
            fields: { temperature: 23.7 },
          };
          connection.write([dataPoint1]).then(() => {
          }, done);
          connection.executeQuery('select * from outdoorThermometerA').then((result) => {
            assert.equal(result.length, 0);
            console.log('assert 1 evaluated');
            setTimeout(() => {
              connection.executeQuery('select * from outdoorThermometerA').then((result2) => {
                assert.equal(result2.length, 1);
                console.log('assert 2 evaluated');
                done();
              }, done);
            }, 1500);
          }, done);
        };
        connection.executeQuery('drop measurement outdoorThermometerA').then(onEmptyMeasurement, onEmptyMeasurement);
      }, done);
    });

    it('should not write measurements to DB before buffer size limit is reached', (done) => {
      const buildPoints = (base, count) => {
        const points = [];
        _.times(count, (i) => {
          points.push({
            measurement: 'outdoorThermometerA',
            timestamp: base + i,
            fields: { temperature: 23.7 },
          });
        });
        return points;
      };

      const connection = new InfluxDB.Connection({
        database: 'test1',
      });

      const onEmptyMeasurement = () => {
        connection.write(buildPoints(0, 200)).then(() => {
          console.log('written 200');
          connection.executeQuery('select * from outdoorThermometerA').then((result) => {
            assert.equal(result.length, 0);
            console.log('assert 1 evaluated');
            connection.write(buildPoints(1000, 900)).then(() => {
              console.log('written 900');
              // these should get written on the next round
              connection.write(buildPoints(2000, 10)).then(() => {
                console.log('written 10');
              }, done);
              setTimeout(() => {
                connection.executeQuery('select * from outdoorThermometerA').then((result2) => {
                  assert.equal(result2.length, 1100);
                  console.log('assert 2 evaluated');
                  done();
                }, done);
              }, 500);
            }, done);
          }, done);
        }, done);
      };

      connection.connect().then(() => {
        connection.executeQuery('drop measurement outdoorThermometerA').then(onEmptyMeasurement, onEmptyMeasurement);
      }, done);
    });
  });
});
