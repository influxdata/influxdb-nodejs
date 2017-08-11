/* global describe it */
import assert from 'assert';
import * as InfluxDB from '~/InfluxDB';
import * as util from '~/../scripts/utils';

describe('Test that types are converted/checked correctly when writing', () => {
  const connection = new InfluxDB.Connection({
    database: 'test1',
    schema: [
      {
        measurement: 'powerf',
        tags: ['location'],
        fields: {
          kwatts: InfluxDB.FieldType.FLOAT,
        },
      },
      {
        measurement: 'powerf2',
        tags: ['location'],
        fields: {
          kwatts: InfluxDB.FieldType.FLOAT,
        },
      },
      {
        measurement: 'testint',
        tags: ['location'],
        fields: {
          rpms: InfluxDB.FieldType.INTEGER,
        },
      },
      {
        measurement: 'teststr',
        tags: ['location'],
        fields: {
          status: InfluxDB.FieldType.STRING,
        },
      },
      {
        measurement: 'testbool',
        tags: ['location'],
        fields: {
          online: InfluxDB.FieldType.BOOLEAN,
        },
      },
    ],

  });

  describe('tags', () => {
    it('should fail on invalid tag', (done) => {
      const dataPoint1 = {
        measurement: 'powerf2',
        tags: { group: 'home' },
        timestamp: new Date().getTime() + 1000000,
        fields: [{ key: 'kwatts', value: 49 }],
      };
      connection.write([dataPoint1]).then(() => {
        done(new Error('should have failed with an error'));
      }, () => {
        done();
      });
    });

    it('should not fail when no tags specified', (done) => {
      const dPF1 = {
        measurement: 'powerf2',
        timestamp: new Date().getTime() + 1000000,
        fields: [{ key: 'kwatts', value: 49 }],
      };
      connection.write([dPF1]).then(done, done);
    });
  });

  describe('floats', () => {
    it('should write floats in all formats', (done) => {
      const dPF1 = {
        measurement: 'powerf',
        timestamp: new Date().getTime() + 1000000,
        tags: [{ key: 'location', value: 'Turbine0017' }],
        fields: [{ key: 'kwatts', value: 49 }],
      };

      const dPF2 = {
        measurement: 'powerf',
        timestamp: new Date().getTime() + 1000000,
        tags: [{ key: 'location', value: 'Turbine0018' }],
        fields: [{ key: 'kwatts', value: 49.013 }],
      };

      const dPF3 = {
        measurement: 'powerf',
        timestamp: new Date().getTime() + 1000000,
        tags: [{ key: 'location', value: 'Turbine0019' }],
        fields: [{ key: 'kwatts', value: 5.009e+1 }],
      };

      connection.connect()
          .then(() => connection.write([dPF1, dPF2, dPF3]))
          .then(() => connection.flush())
          .then(done, done);
    });

    it('should verify the field type in Influx', (done) => {
      connection.connect()
          .then(() => connection.executeQuery('SHOW FIELD KEYS'))
          .then((result) => {
            assert.equal(util.getFieldType(result, 'kwatts'), 'float');
            done();
          }, done);
    });

    it('should read back the float values', (done) => {
      connection.connect()
          .then(() => connection.executeQuery('select * from powerf'))
          .then((result) => {
            assert.equal(result.length, 3);
            result.forEach((elem) => {
              switch (elem.location) {
                case 'Turbine0017': // 49
                  assert.equal(elem.kwatts, 49.0);
                  break;
                case 'Turbine0018': // 49.013
                  assert.equal(elem.kwatts, 49.013);
                  break;
                case 'Turbine0019': // 5.009e+1
                  assert.equal(elem.kwatts, 50.09);
                  break;
                default:
                  assert.fail(elem.kwatts, elem.location,
                      'unexpected element in results array', ',');
                  break;
              }
            });
            done();
          }, done);
    });

    it('should drop the float values', (done) => {
      done(util.dropMeasurement(connection, 'powerf'));
    });
  });

  describe('Integers', () => {
    it('should write integers', (done) => {
      const dPI1 = {
        measurement: 'testint',
        timestamp: new Date(),
        tags: [{ key: 'location', value: 'Turbine0017' }],
        fields: [{ key: 'rpms', value: 49 }],
      };

      const dPI2 = {
        measurement: 'testint',
        timestamp: new Date(),
        tags: [{ key: 'location', value: 'Turbine0018' }],
        fields: [{ key: 'rpms', value: Number.MAX_SAFE_INTEGER }],
      };

      const dPI3 = {
        measurement: 'testint',
        timestamp: new Date(),
        tags: [{ key: 'location', value: 'Turbine0019' }],
        fields: [{ key: 'rpms', value: Number.MIN_SAFE_INTEGER }],
      };

      connection.connect()
          .then(() => connection.write([dPI1, dPI2, dPI3]))
          .then(() => connection.flush())
          .then(done, done);
    });

    it('should verify the field type in Influx', (done) => {
      connection.connect()
          .then(() => connection.executeQuery('SHOW FIELD KEYS'))
          .then((result) => {
            assert.equal(util.getFieldType(result, 'rpms'), 'integer');
            done();
          }, done);
    });

    it('should read back the integer values', (done) => {
      connection.connect()
          .then(() => connection.executeQuery('select * from testint'))
          .then((result) => {
            assert.equal(result.length, 3);
            result.forEach((dp) => {
              switch (dp.location) {
                case 'Turbine0017': // 49
                  assert.equal(dp.rpms, 49);
                  break;
                case 'Turbine0018': // MAX_INT
                  assert.equal(dp.rpms, Number.MAX_SAFE_INTEGER);
                  break;
                case 'Turbine0019': // MIN_INT
                  assert.equal(dp.rpms, Number.MIN_SAFE_INTEGER);
                  break;
                default:
                  assert.fail(dp.rpms, dp.location,
                      'unexpected element in results array', ',');
                  break;
              }
            });
            done();
          })
          .catch(done);
    });

    it('should drop the integer values', (done) => {
      done(util.dropMeasurement(connection, 'testint'));
    });
  });

  describe('Strings', () => {
    const stringDataPoint1 = {
      measurement: 'teststr',
      timestamp: new Date(),
      tags: [{ key: 'location', value: 'Turbine0017' }],
      fields: [{ key: 'status', value: 'OK' }],
    };

    const stringDataPoint2 = {
      measurement: 'teststr',
      timestamp: new Date(),
      tags: [{ key: 'location', value: 'Turbine0018' }],
      fields: [{ key: 'status', value: 'WARNING' }],
    };

    const stringDataPoint3 = {
      measurement: 'teststr',
      timestamp: new Date(),
      tags: [{ key: 'location', value: 'Turbine0019' }],
      fields: [{ key: 'status', value: 'OFFLINE' }],
    };

    it('should write legitimate strings', (done) => {
      connection.connect()
          .then(() => connection.write([stringDataPoint1, stringDataPoint2, stringDataPoint3]))
          .then(() => connection.flush())
          .then(() => {
            done();
          })
          .catch(done);
    });

    it('should verify the field type in Influx', (done) => {
      connection.connect()
          .then(() => connection.executeQuery('SHOW FIELD KEYS'))
          .then((result) => {
            assert.equal(util.getFieldType(result, 'status'), 'string');
            done();
          }, done);
    });

    it('should read the strings back', (done) => {
      connection.connect()
          .then(() => connection.executeQuery('select * from teststr'))
          .then((result) => {
            assert.equal(result.length, 3);
            result.forEach((dp) => {
              switch (dp.location) {
                case 'Turbine0017':
                  assert.equal(dp.status, 'OK');
                  break;
                case 'Turbine0018':
                  assert.equal(dp.status, 'WARNING');
                  break;
                case 'Turbine0019':
                  assert.equal(dp.status, 'OFFLINE');
                  break;
                default:
                  assert.fail(dp.status, dp.location,
                      'unexpected element in results array', ',');
                  break;
              }
            });
            done();
          }, done);
    });

    it('should drop the datapoints', (done) => {
      done(util.dropMeasurement(connection, 'teststr'));
    });
  });

  describe('Bools', () => {
    const boolDataPoint1 = {
      measurement: 'testbool',
      timestamp: new Date(),
      tags: [{ key: 'location', value: 'Turbine0001' }],
      fields: [{ key: 'online', value: true }],
    };

    const boolDataPoint2 = {
      measurement: 'testbool',
      timestamp: new Date(),
      tags: [{ key: 'location', value: 'Turbine0002' }],
      fields: [{ key: 'online', value: false }],
    };

    const boolDataPoint3 = {
      measurement: 'testbool',
      timestamp: new Date(),
      tags: [{ key: 'location', value: 'Turbine0003' }],
      fields: [{ key: 'online', value: true }],
    };

    const boolDataPoint4 = {
      measurement: 'testbool',
      timestamp: new Date(),
      tags: [{ key: 'location', value: 'Turbine0005' }],
      fields: [{ key: 'online', value: false }],
    };
    const boolDataPoint5 = {
      measurement: 'testbool',
      timestamp: new Date(),
      tags: [{ key: 'location', value: 'Turbine0007' }],
      fields: [{ key: 'online', value: true }],
    };

    it('should write booleans', (done) => {
      connection.connect()
          .then(() => connection.write([boolDataPoint1, boolDataPoint2, boolDataPoint3,
            boolDataPoint4, boolDataPoint5]))
          .then(() => connection.flush()).then(done, done);
    });

    it('should verify the field type in Influx', (done) => {
      connection.connect()
          .then(() => connection.executeQuery('SHOW FIELD KEYS'))
          .then((result) => {
            assert.equal(util.getFieldType(result, 'online'), 'boolean');
            done();
          }).catch(done);
    });

    it('should read the booleans back', (done) => {
      connection.connect()
          .then(() => connection.executeQuery('select * from testbool'))
          .then((result) => {
            assert.equal(result.length, 5);
            result.forEach((dp) => {
              switch (dp.location) {
                case 'Turbine0001':
                case 'Turbine0003':
                case 'Turbine0007':
                  assert.equal(dp.online, true);
                  break;
                case 'Turbine0002':
                case 'Turbine0005':
                  assert.equal(dp.online, false);
                  break;
                default:
                  assert.fail(dp.status, dp.location,
                      'unexpected element in results array', ',');
                  break;
              }
            });
            done();
          })
          .catch(done);
    });

    it('should drop the datapoints', (done) => {
      done(util.dropMeasurement(connection, 'testbool'));
    });
  });

  describe('Conflict string to float', () => {
    const connection2 = new InfluxDB.Connection({
      database: 'test2',
      schema: [
        {
          measurement: 'current',
          tags: ['location'],
          fields: {
            volts: InfluxDB.FieldType.FLOAT,
          },
        }],
    });

    const floatDataPoint = {
      measurement: 'current',
      timestamp: new Date().getTime() + 1000000,
      tags: [{ key: 'location', value: 'Turbine0017' }],
      fields: [{ key: 'volts', value: 271 }],
    };

    const stringDataPoint = {
      measurement: 'current',
      timestamp: new Date().getTime() + 1000000,
      tags: [{ key: 'location', value: 'Turbine0017' }],
      fields: [{ key: 'volts', value: 'forty-seven' }],
    };

    it('should write the initial float', (done) => {
      const connectionPromise = connection2.connect();
      connectionPromise.then(() => connection2.write([floatDataPoint])).then(done, done);
    });

    it('should catch the invalid string type', (done) => {
      const connectionPromise = connection2.connect();
      connectionPromise.then(() => connection2.write([stringDataPoint])).then(() => {
        done(new Error('Type check should fail'));
      }).catch(() => {
        done();
      });
    });

    it('should drop the float data', (done) => {
      done(util.dropMeasurement(connection2, 'current'));
    });
  });

  describe('Conflict float to integer', () => {
    const connection3 = new InfluxDB.Connection({
      database: 'test2',
      schema: [
        {
          measurement: 'pulse',
          tags: ['location'],
          fields: {
            bps: InfluxDB.FieldType.INTEGER,
          },
        }],
    });

    const integerDataPoint = {
      measurement: 'pulse',
      timestamp: new Date().getTime() + 1000000,
      tags: [{ key: 'location', value: 'Turbine0017' }],
      fields: [{ key: 'bps', value: 271 }],
    };

    const floatDataPoint = {
      measurement: 'pulse',
      timestamp: new Date().getTime() + 1000000,
      tags: [{ key: 'location', value: 'Turbine0017' }],
      fields: [{ key: 'bps', value: 302.5432 }],
    };

    it('should write the initial int', (done) => {
      connection3.write([integerDataPoint]).then(done, done);
    });

    it('should detect writing float into integer field', (done) => {
      const connectionPromise = connection3.connect();
      connectionPromise.then(() => connection3.write([floatDataPoint])).then(() => {
        done(new Error('Type check should fail'));
      }).catch(() => {
        done();
      });
    });

    it('should drop the integer data', (done) => {
      done(util.dropMeasurement(connection3, 'pulse'));
    });
  });

  describe('Conflict float to boolean', () => {
    const connection4 = new InfluxDB.Connection({
      database: 'test2',
      schema: [
        {
          measurement: 'connection',
          tags: ['location'],
          fields: {
            connected: InfluxDB.FieldType.BOOLEAN,
          },
        }],
    });

    const boolDataPoint = {
      measurement: 'connection',
      timestamp: new Date().getTime() + 1000000,
      tags: [{ key: 'location', value: 'Turbine0017' }],
      fields: [{ key: 'connected', value: true }],
    };

    const floatDataPoint = {
      measurement: 'connection',
      timestamp: new Date().getTime() + 1000000,
      tags: [{ key: 'location', value: 'Turbine0017' }],
      fields: [{ key: 'connected', value: 0.0012 }],
    };

    it('should write the initial bool', (done) => {
      const connectionPromise = connection4.connect();
      connectionPromise.then(() => connection4.write([boolDataPoint])).then(done, done);
    });

    it('should catch the invalid float type', (done) => {
      const connectionPromise = connection4.connect();
      connectionPromise.then(() => connection4.write([floatDataPoint])).then(() => {
        done(new Error('Type check should fail'));
      }).catch(() => {
        done();
      });
    });

    it('should drop the boolean data', (done) => {
      done(util.dropMeasurement(connection4, 'connection'));
    });
  });

  describe('Conflict float to string', () => {
    const connection5 = new InfluxDB.Connection({
      database: 'test2',
      schema: [
        {
          measurement: 'status',
          tags: ['location'],
          fields: {
            state: InfluxDB.FieldType.STRING,
          },
        }],
    });

    const stringDataPoint = {
      measurement: 'status',
      timestamp: new Date().getTime() + 1000000,
      tags: [{ key: 'location', value: 'Turbine0017' }],
      fields: [{ key: 'state', value: 'WARNING' }],
    };

    const floatDataPoint = {
      measurement: 'status',
      timestamp: new Date().getTime() + 1000000,
      tags: [{ key: 'location', value: 'Turbine0017' }],
      fields: [{ key: 'state', value: 7 }],
    };

    it('should write the initial string', (done) => {
      const connectionPromise = connection5.connect();
      connectionPromise.then(() => connection5.write([stringDataPoint])).then(done, done);
    });

    it('should catch the invalid float type', (done) => {
      const connectionPromise = connection5.connect();
      connectionPromise.then(() => connection5.write([floatDataPoint])).then(() => {
        done(new Error('Type check should fail'));
      }).catch(() => {
        done();
      });
    });

    it('should drop the string data', (done) => {
      done(util.dropMeasurement(connection5, 'status'));
    });
  });
});
