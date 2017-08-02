/* global describe it */
import assert from 'assert';
import * as InfluxDB from '~/InfluxDB';

describe('InfluxDB.timestamps', () => {
  describe('#timestamps check', () => {
    const testDate = new Date();

    // Timestamp type number
    const dpTS1 = {
      measurement: 'powerts',
      timestamp: testDate.getTime(),
      tags: [{ key: 'location', value: 'Turbine003' }],
      fields: [{ key: 'kwatts', value: 47 }],
    };

    // Timestamp type object
    const dpTS2 = {
      measurement: 'powerts',
      timestamp: testDate,
      tags: [{ key: 'location', value: 'Turbine005' }],
      fields: [{ key: 'kwatts', value: 46.5 }],
    };

    // Timestamp type String
    const dpTS3 = {
      measurement: 'powerts',
      timestamp: testDate,
      tags: [{ key: 'location', value: 'Turbine007' }],
      fields: [{ key: 'kwatts', value: 48.33 }],
    };


    const connection = new InfluxDB.Connection({
      database: 'test1',
    });

    it('should store datapoint with same timestamps', (done) => {
      connection.connect().then(() => {
        connection.write([dpTS1, dpTS2, dpTS3]).then(() => {
          connection.flush().then(() => {
            done();
          }).catch((e) => {
            done(e);
          });
        }).catch((e) => {
          done(e);
        });
      }).catch((e) => {
        done(e);
      });
    });

    it('should read back the same timestamps', (done) => {
      connection.connect().then(() => {
        connection.executeQuery('SELECT * FROM powerts').then((result) => {
          assert.equal(result.length, 3);
          result.forEach((element) => {
            const td = new Date(element.time);
            assert(td.getTime() === testDate.getTime(), `Returned time ${td}
                              does not equal original test time ${testDate} ${element.location}`);
          });
          done();
        }).catch((e) => {
          done(e);
        });
      }).catch((e) => {
        done(e);
      });
    });

    it('should handle undefined timestamps, autoGenerateTimestamps=false', (done) => {
      const cnx = new InfluxDB.Connection({
        database: 'test1',
        autoGenerateTimestamps: false,
      });
      cnx.connect().then(() => {
        cnx.write({
          measurement: 'powerts',
          tags: [{ key: 'location', value: 'Turbine00312' }],
          fields: [{ key: 'kwatts', value: 47 }],
        }).then(() => {
          cnx.flush().then(() => {
            done();
          }).catch((e) => {
            done(e);
          });
        }).catch((e) => {
          done(e);
        });
      }).catch((e) => {
        done(e);
      });
    });


    it('should handle undefined timestamps,autoGenerateTimestamps=true', (done) => {
      const cnx = new InfluxDB.Connection({
        database: 'test1',
        autoGenerateTimestamps: true,
      });
      cnx.connect().then(() => {
        cnx.write({
          measurement: 'powerts',
          tags: [{ key: 'location', value: 'Turbine00312' }],
          fields: [{ key: 'kwatts', value: 47 }],
        }).then(() => {
          cnx.flush().then(() => {
            done();
          }).catch((e) => {
            done(e);
          });
        }).catch((e) => {
          done(e);
        });
      }).catch((e) => {
        done(e);
      });
    });

    it('should drop the test datapoints', (done) => {
      connection.connect().then(() => {
        connection.executeQuery('DROP measurement powerts').then(() => {
          done();
        }).catch((e) => {
          done(e);
        });
      }).catch((e) => {
        done(e);
      });
    });
  });
});
