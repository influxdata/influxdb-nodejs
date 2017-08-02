/* global describe it */
import assert from 'assert';
import * as InfluxDB from '~/InfluxDB';
import * as util from '~/../scripts/utils';

describe('Connection test', () => {
  describe('#Automatic connect and write', () => {
    const cxnaw = new InfluxDB.Connection({

      database: 'test1',

    });

    const dp1 = {
      measurement: 'location',
//            timestamp: new Date(), //N.B. client should fill in missing timestamp automatically
      tags: [{ key: 'unit', value: 'adam-12' }],
      fields: [{ key: 'coordinates', value: '34_06_46_N_118_20_20_W' }],
    };

    const dp2 = {
      measurement: 'location',
//            timestamp: new Date(),
      tags: [{ key: 'unit', value: 'l-30' }],
      fields: [{ key: 'coordinates', value: '34.11856_N_118.30037_W' }],
    };

    const dp3 = {
      measurement: 'location',
//            timestamp: new Date(),
      tags: [{ key: 'unit', value: 'zebra-07' }],
      fields: [{ key: 'coordinates', value: '33_56_33_N_118_24_29_W' }],
    };

    it('should automatically connect and write data points, incl. timestamp', (done) => {
      cxnaw.write([dp1, dp2, dp3], true).then(() => {
        done();
      }).catch((e) => {
        done(e);
      });
    });

    it('should read back and verify the data', (done) => {
      util.sleep(500)
        .then(() => cxnaw.executeQuery('Select * from location'))
        .then((result) => {
          assert.equal(result.length, 3);
          result.forEach((dp) => {
            assert(dp.time !== undefined);
            switch (dp.unit) {
              case 'adam-12':
                assert.equal(dp.coordinates, dp1.fields[0].value);
                break;
              case 'l-30':
                assert.equal(dp.coordinates, dp2.fields[0].value);
                break;
              case 'zebra-07':
                assert.equal(dp.coordinates, dp3.fields[0].value);
                break;
              default:
                assert.fail(dp.unit, dp.coordinates, 'Unexpected element in results array');
                break;
            }
          });
          done();
        })
        .catch((e) => {
          done(e);
        });
    });

    it('should drop the measurement', (done) => {
      const result = util.dropMeasurement(cxnaw, 'location');

// take a moment for transaction to complete otherwise lose connection object too soon in some cases
      util.sleep(1000).then(() => { done(result); });
    });
  });

  describe('#Cache size automatic write', () => {
    const cxnsm = new InfluxDB.Connection({

      database: 'test1',
      batchSize: 100,
      maximumWriteDelay: 5000, // set longer write delay, ensure auto write triggered by buffer size

    });

    const testdps = util.buildDatapoints('distance',
      [{ name: 'unit', base: 'baker-', type: 'string' }],
      [{ name: 'frombase', base: 0, type: 'float' }],
      300); // force buffer to flush three times in succession
    const chunkSize = 10;
    // will write in chunks of 10 items
    const dpchunks = testdps
      .map((e, i) => (i % chunkSize === 0 ? testdps.slice(i, i + chunkSize) : null))
      .filter(e => e);

    it('should write successive chunks to buffer and trigger write to db', (done) => {
      cxnsm.connect().then(() => {
        dpchunks.forEach((chunk) => {
          cxnsm.write(chunk).catch((e) => {
            done(e);
          });
        });
        done();
      }).catch((e) => {
        done(e);
      });
    });

    it('should read back the data', (done) => {
      cxnsm.connect()
        .then(() => cxnsm.executeQuery('SELECT * FROM distance'))
        .then((result) => {
          assert(result.length, testdps.length);
          done();
        })
        .catch((e) => {
          done(e);
        });
    });

    it('shoud drop the test measurement', (done) => {
      const result = util.dropMeasurement(cxnsm, 'distance');
      // take a moment for transaction to complete
      // otherwise lose connection object too soon in some cases
      util.sleep(1000).then(() => { done(result); });
    });
  });

  describe('#Timeout expire automatic write', () => {
    const writeDelay = 1000; // default writeDelay

    const cxnquick = new InfluxDB.Connection({

      database: 'test1',
      maximumWriteDelay: writeDelay,

    });

    const dp1 = {
      measurement: 'temperature',
      timestamp: new Date(), // N.B. client should fill in missing timestamp automatically
      tags: [{ key: 'turbine', value: 'bremerhaven-0013' }],
      fields: [{ key: 'celsius', value: '67.3' }],
    };

    const dp2 = {
      measurement: 'temperature',
      timestamp: new Date(),
      tags: [{ key: 'turbine', value: 'bremerhaven-0017' }],
      fields: [{ key: 'celsius', value: '22' }],
    };

    const dp3 = {
      measurement: 'temperature',
      timestamp: new Date(),
      tags: [{ key: 'turbine', value: 'bremerhaven-0019' }],
      fields: [{ key: 'celsius', value: '39.5' }],
    };

    it('should write to db after delay expires', (done) => {
      cxnquick.connect()
        .then(() => cxnquick.write([dp1, dp2, dp3]))
        .then(() => {
          done();
        })
        .catch((e) => {
          done(e);
        });
    });

    it(`should read back the data after the delay of ${writeDelay}ms expires`, (done) => {
      cxnquick.connect()
        .then(() => util.sleep(writeDelay))
        .then(() => cxnquick.executeQuery('Select * from temperature'))
        .then((result) => {
          assert.equal(result.length, 3);
          result.forEach((dp) => {
            switch (dp.turbine) {
              case 'bremerhaven-0013':
                assert.equal(dp.celsius, dp1.fields[0].value);
                break;
              case 'bremerhaven-0017':
                assert.equal(dp.celsius, dp2.fields[0].value);
                break;
              case 'bremerhaven-0019':
                assert.equal(dp.celsius, dp3.fields[0].value);
                break;
              default:
                throw new Error('Unknown element in results array');
            }
          });
          done();
        })
        .catch((e) => {
          done(e);
        });
    });

    it('should drop the test data', (done) => {
      const result = util.dropMeasurement(cxnquick, 'temperature');
      util.sleep(1000).then(() => {
        done(result);
      });
    });
  });

    /*
       From Connection Configuration esdoc...
       1. should reliably write to influxdb
       2. no need to call flush, wait for buffer to fill or timeout - returns only after write
       3. should be slower than when set to true
     */
  describe('#autoResolvePromissedWritesToCache - false', () => {
    const cxnauto = new InfluxDB.Connection({
      database: 'test1',
      autoResolvePromisedWritesToCache: true,
    });

    const cxnwait = new InfluxDB.Connection({
      database: 'test1',
      autoResolvePromisedWritesToCache: false,
    });


    const dps = util.buildDatapoints('temp',
            [{ name: 'thermometer', base: 'tmeter', type: 'string' }],
            [{ name: 'cels', base: 17, type: 'float' }],
            3000);

    let autoWriteTime = 0;
    let start;

        // get initial time of autoResolvePromisedWritesToCache: true for later comparison
    cxnauto.connect()
      .then(() => {
        start = new Date().getTime();
        return cxnauto.write(dps);
      })
      .then(() => {
        const end = new Date().getTime();
        autoWriteTime = end - start;
      })
      .catch((e) => {
        console.log('error', e);
      });

    util.dropMeasurement(cxnauto, 'temp');
        // take a moment for transaction to complete
    // otherwise lose connection object too soon in some cases
    util.sleep(2000).then(() => { }); // should be dropped

    it('should write points to server and then return promise', (done) => {
      let waitWriteTime = 0;
      let waitStart;
      cxnwait.connect()
        .then(() => {
          waitStart = new Date().getTime();
          return cxnwait.write(dps);
        })
        .then(() => {
          const end = new Date().getTime();
          waitWriteTime = end - waitStart;
          console.log(`test ${util.pad(31, 10, '0')}`);
          console.log(`autoResolvePromisedWritesToCache(false) ${util.pad(waitWriteTime, 6, ' ')}ms`);
          console.log(`autoResolvePromisedWritesToCache(true)  ${util.pad(autoWriteTime, 6, ' ')}ms`);
          console.log(`autoResolvePromisedWritesToCache(diff)  ${util.pad(waitWriteTime - autoWriteTime, 6, ' ')}ms`);
          assert(waitWriteTime > autoWriteTime);
          done();
        })
        .catch((e) => {
          done(e);
        });
    });

    it('should read the points back', (done) => {
      cxnwait.connect()
        .then(() => cxnwait.executeQuery('select * from temp'))
        .then((result) => {
          assert(result.length === dps.length);
          done();
        })
        .catch((e) => {
          done(e);
        });
    });

    it('should drop the datapoints', (done) => {
      const result = util.dropMeasurement(cxnwait, 'temp');
      // take a moment for transaction to complete
      // otherwise lose connection object too soon in some cases
      util.sleep(1000).then(() => { done(result); });
    });
  });


  describe('#autoCreateDatabase: false', () => {
    const existingdb = 'reified';
    const nonexistingdb = 'phantomdb';

    const cxnnoexist = new InfluxDB.Connection({
      database: nonexistingdb,
      autoCreateDatabase: false,
    });

    const cxnsetup = new InfluxDB.Connection({
      database: existingdb,
      autoCreateDatabase: true,
    });

    const cxnexist = new InfluxDB.Connection({
      database: existingdb,
      autoCreateDatabase: false,
    });

    const testdp = {
      measurement: 'flips',
      timestamp: new Date(),
      tags: [{ key: 'turbine', value: 'bremerhaven-0019' }],
      fields: [{ key: 'flops', value: '99' }],
    };

    it('should set up a preexisting database', (done) => {
      cxnsetup.connect().then(() => {
        done();
      }).catch((e) => {
        done(e);
      });
    });


    it('should fail to write to a non-existant database', (done) => {
      cxnnoexist.connect()
        .then(() => cxnnoexist.write([testdp]))
        .then(() => cxnnoexist.flush())
        .then(() => cxnnoexist.executeQuery('SHOW DATABASES'))
        .then((result) => {
          done(new Error(`No error on write to the nonexistant database ${nonexistingdb}. Current Databases are ${JSON.stringify(result)}`));
        })
        .catch((e) => {
// done(new Error(`No error on write to the nonexistant database ${nonexistingdb}. ${e}`))
          done(e);
        });
    });

    it('should write to an existing database', (done) => {
      cxnexist.connect()
        .then(() => cxnexist.write([testdp], true))
        .then(() => {
          done();
        })
        .catch((e) => {
          done(e);
        });
    });

    it('should read back the data from the existing database', (done) => {
      util.sleep(500)
        .then(() => cxnexist.connect())
        .then(() => cxnexist.executeQuery('select * from flips'))
        .then((result) => {
          try {
            assert(result.length > 0);
            done();
          } catch (e) {
            done(e);
          }
        })
        .catch((e) => {
          done(e);
        });
    });

    it('should drop the database just created', (done) => {
            // wait to make sure other operations have completed
      util.sleep(1000)
        .then(() => cxnsetup.connect())
        .then(() => cxnsetup.executeQuery(`DROP DATABASE ${existingdb}`))
        .then(() => {
          done();
        })
        .catch((e) => {
          done(e);
        });
    });
  });

  describe('#Disable batching', () => {
    it('should automatically write when batch size is 0', (done) => {
      const cxnbatch0 = new InfluxDB.Connection({
        database: 'test2',
        batchSize: 0,
      });

      const testdp = {
        measurement: 'flips',
        timestamp: new Date(),
        tags: [{ key: 'turbine', value: 'bremerhaven-0019' }],
        fields: [{ key: 'flops', value: '99' }],
      };

      cxnbatch0.connect()
        .then(() => cxnbatch0.write([testdp]))
        .then(() => util.sleep(500))
        .then(() => cxnbatch0.executeQuery('SELECT * FROM flips'))
        .then((result) => {
          try {
            assert(result.length > 0);
            done(util.dropMeasurement(cxnbatch0, 'flips'));
          } catch (e) {
            done(e);
          }
        })
        .catch((e) => {
          done(e);
        });
    });

    it('Should automatically write when minimumWriteDelay is 0', (done) => {
      const cxndelay0 = new InfluxDB.Connection({
        database: 'test2',
        maximumWriteDelay: 0,
      });

      const testdp = {
        measurement: 'flops',
        timestamp: new Date(),
        tags: [{ key: 'turbine', value: 'bremerhaven-0019' }],
        fields: [{ key: 'flips', value: '99' }],
      };

      cxndelay0.connect()
        .then(() => cxndelay0.write([testdp]))
        .then(() => util.sleep(500))
        .then(() => cxndelay0.executeQuery('SELECT * FROM flops'))
        .then((result) => {
          try {
            assert(result.length > 0);
            done(util.dropMeasurement(cxndelay0, 'flops'));
          } catch (e) {
            done(e);
          }
        })
        .catch((e) => {
          done(e);
        });
    });
  });

    /*
    2017.07.20 not yet implemented

    describe("Connect over UDP", function(){

        let cxnudp = new InfluxDB.Connection({
            database: 'udptest',
            hostUrl: 'udp://127.0.0.1:8089'
        });

        let testdps = util.buildDatapoints('effective-light',
            [{name: 'array', base: 'baker-', type: 'string'}],
            [{name: 'lumens', base: 0, type: 'float'}],
            300) //force buffer to flush three times in succession


        it('shoud write to the database over udp', function(done){

            cxnudp.connect().then(() => {
                cxnudp.write(testdps).then(() => {
                    cxnudp.flush().then(() => {
                        cxnudp.executeQuery('Select * from effective-light').then((result) => {
                            //done(util.dropMeasurement(cxnudp))
                        }).catch((e) => {
                            done(e)
                        })
                    }).catch((e) => {
                        done(e)
                    })
                }).catch((e) => {
                    done(e)
                })
            }).catch((e) => {
                done(e)
            })

        })

    });
    */
});
