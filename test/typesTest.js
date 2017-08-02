/* global describe it */
import assert from 'assert';
import * as InfluxDB from '~/InfluxDB'
import * as util from '~/../scripts/utils'

describe('InfluxDB.types', () => {
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
      const dPF1 = {
        measurement: 'powerf2',
        tags: { group: 'home' },
        timestamp: new Date().getTime() + 1000000,
        fields: [{ key: 'kwatts', value: 49 }],
      };
      const cxnp = connection.connect();
      const writep = cxnp.then(() => connection.write([dPF1]));
      const flushp = writep.then(() => connection.flush());

      flushp.then(() => {
        done(new Error('tags are not properly checked'));
      });

      writep.catch((e) => {
        console.log('Caught schema violation');
        console.log(e);
        done();
      });

      // connection issue
      cxnp.catch((e) => {
        done(e);
      });
    });

    it('should not fail when no tags specified', (done) => {
      const dPF1 = {
        measurement: 'powerf2',
        timestamp: new Date().getTime() + 1000000,
        fields: [{ key: 'kwatts', value: 49 }],
      };
      connection.connect()
        .then(() => connection.write([dPF1]))
        .then(() => connection.flush())
        .then(() => {
          done();
        })
        .catch((e) => {
          done(e);
        });
    });
  });

  describe('#floats', () => {
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
        .then(() => {
          done();
        })
        .catch((e) => {
          done(e);
        });
    });

    it('should verify the field type in Influx', (done) => {
      connection.connect()
        .then(() => connection.executeQuery('SHOW FIELD KEYS'))
        .then((result) => {
          assert.equal(util.getFieldType(result, 'kwatts'), 'float');
          done();
        })
        .catch((e) => {
          done(e);
        });
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
        })
        .catch((e) => {
          done(e);
        });
    });

    it('should drop the float values', (done) => {
      done(util.dropMeasurement(connection, 'powerf'));
    });
  });

  describe('#Integers', () => {
    it('should write integers', (done) => {
      const dPI1 = {
        measurement: 'testint',
        timestamp: new Date(),
        tags: [{ key: 'location', value: 'Turbine0017' }],
        fields: [{ key: 'rpms', value: 49 }], // , type: FieldType.INTEGER}]
      };

      const dPI2 = {
        measurement: 'testint',
        timestamp: new Date(),
        tags: [{ key: 'location', value: 'Turbine0018' }],
        fields: [{ key: 'rpms', value: Number.MAX_SAFE_INTEGER }], // , type: FieldType.INTEGER}]
      };

      const dPI3 = {
        measurement: 'testint',
        timestamp: new Date(),
        tags: [{ key: 'location', value: 'Turbine0019' }],
        fields: [{ key: 'rpms', value: Number.MIN_SAFE_INTEGER }], // , type: FieldType.INTEGER}]
      };

      connection.connect()
        .then(() => connection.write([dPI1, dPI2, dPI3]))
        .then(() => connection.flush())
        .then(() => {
          done();
        })
        .catch((e) => {
          done(e);
        });
    });

    it('should verify the field type in Influx', (done) => {
      connection.connect()
        .then(() => connection.executeQuery('SHOW FIELD KEYS'))
        .then((result) => {
          assert.equal(util.getFieldType(result, 'rpms'), 'integer');
          done();
        }).catch((e) => {
          done(e);
        });
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
        .catch((e) => {
          done(e);
        });
    });

    it('should drop the integer values', (done) => {
      done(util.dropMeasurement(connection, 'testint'));
    });
  });

  describe('#Strings', () => {
//  N.B. these values were originally conceived for tests of escaping strings that could
//  present a security risk. This use case is not practical at this time.
//    const jsonTestStr = '{ type: \'mischief\', id: \'escape-or-fail-me\', volts: 12.3 }';
//    const qlTestStr = 'SELECT * FROM moremischief';

    const dpS1 = {
      measurement: 'teststr',
      timestamp: new Date(),
      tags: [{ key: 'location', value: 'Turbine0017' }],
      fields: [{ key: 'status', value: 'OK' }],
    };

    const dpS2 = {
      measurement: 'teststr',
      timestamp: new Date(),
      tags: [{ key: 'location', value: 'Turbine0018' }],
      fields: [{ key: 'status', value: 'WARNING' }],
    };

    const dpS3 = {
      measurement: 'teststr',
      timestamp: new Date(),
      tags: [{ key: 'location', value: 'Turbine0019' }],
      fields: [{ key: 'status', value: 'OFFLINE' }],
    };

    it('should write legitimate strings', (done) => {
      connection.connect()
        .then(() => connection.write([dpS1, dpS2, dpS3]))
        .then(() => connection.flush())
        .then(() => {
          done();
        })
        .catch((e) => {
          done(e);
        });
    });

    it('should verify the field type in Influx', (done) => {
      connection.connect()
        .then(() => connection.executeQuery('SHOW FIELD KEYS'))
        .then((result) => {
          assert.equal(util.getFieldType(result, 'status'), 'string');
          done();
        }).catch((e) => {
          done(e);
        });
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
        })
        .catch((e) => {
          done(e);
        });
    });

    it('should drop the datapoints', (done) => {
      done(util.dropMeasurement(connection, 'teststr'));
    });
  });

  describe('#Bools', () => {
    const dpB1 = {
      measurement: 'testbool',
      timestamp: new Date(),
      tags: [{ key: 'location', value: 'Turbine0001' }],
      fields: [{ key: 'online', value: true }],
    };

    const dpB2 = {
      measurement: 'testbool',
      timestamp: new Date(),
      tags: [{ key: 'location', value: 'Turbine0002' }],
      fields: [{ key: 'online', value: false }],
    };

    const dpB3 = {
      measurement: 'testbool',
      timestamp: new Date(),
      tags: [{ key: 'location', value: 'Turbine0003' }],
      fields: [{ key: 'online', value: true }], // 1}]
    };

    const dpB4 = {
      measurement: 'testbool',
      timestamp: new Date(),
      tags: [{ key: 'location', value: 'Turbine0005' }],
      fields: [{ key: 'online', value: false }], // 0}]
    };
// This should throw an exeception on write - field should be type bool
// Need to test and catch this separately as check that field type is bool
    const dpB5 = {
      measurement: 'testbool',
      timestamp: new Date(),
      tags: [{ key: 'location', value: 'Turbine0007' }],
      fields: [{ key: 'online', value: true }], // 99}]
    };

    it('should write booleans', (done) => {
      connection.connect()
        .then(() => connection.write([dpB1, dpB2, dpB3, dpB4, dpB5]))
        .then(() => connection.flush())
        .then(() => {
          done();
        })
        .catch((e) => {
          done(e);
        });
    });

    it('should verify the field type in Influx', (done) => {
      connection.connect()
        .then(() => connection.executeQuery('SHOW FIELD KEYS'))
        .then((result) => {
          assert.equal(util.getFieldType(result, 'online'), 'boolean');
          done();
        }).catch((e) => {
          done(e);
        });
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
        .catch((e) => {
          done(e);
        });
    });

    it('should drop the datapoints', (done) => {
      done(util.dropMeasurement(connection, 'testbool'));
    });
  });

  describe('#Conflict string to float', () => {
    const cxnf = new InfluxDB.Connection({
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

    const dpFlt = {
      measurement: 'current',
      timestamp: new Date().getTime() + 1000000,
      tags: [{ key: 'location', value: 'Turbine0017' }],
      fields: [{ key: 'volts', value: 271 }],
    };

    const dpStr = {
      measurement: 'current',
      timestamp: new Date().getTime() + 1000000,
      tags: [{ key: 'location', value: 'Turbine0017' }],
      fields: [{ key: 'volts', value: 'forty-seven' }],
    };

    it('should write the initial float', (done) => {
      cxnf.connect()
        .then(() => cxnf.write([dpFlt]))
        .then(() => cxnf.flush())
        .then(() => {
          done();
        })
        .catch((e) => {
          done(e);
        });
    });

    it('should catch the invalid string type', (done) => {
      const cxnp = cxnf.connect();
      const writep = cxnp.then(() => cxnf.write([dpStr]));
      const flushp = writep.then(() => cxnf.flush());
      writep.catch((e) => {
        console.log(JSON.stringify(e));
        done();
      });

      flushp.then(() => {
        done(new Error('Managed to write value of type String to field of type Float'));
      });
      flushp.catch((e) => {
//      console.log('second write flush failed - which is correct: ' + e);
        console.log(JSON.stringify(e));
        assert(e.message !== undefined);
        assert(e.data !== undefined);
        done();
      });
    });

    it('should drop the float data', (done) => {
      done(util.dropMeasurement(cxnf, 'current'));
    });
  });

  describe('#Conflict float to integer', () => {
    const cxni = new InfluxDB.Connection({
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

    const dpInt = {
      measurement: 'pulse',
      timestamp: new Date().getTime() + 1000000,
      tags: [{ key: 'location', value: 'Turbine0017' }],
      fields: [{ key: 'bps', value: 271 }],
    };

    const dpFlt = {
      measurement: 'pulse',
      timestamp: new Date().getTime() + 1000000,
      tags: [{ key: 'location', value: 'Turbine0017' }],
      fields: [{ key: 'bps', value: 302.5432 }],
    };

    it('should write the initial int', (done) => {
      cxni.connect()
        .then(() => cxni.write([dpInt]))
        .then(() => cxni.flush())
        .then(() => {
          done();
        })
        .catch((e) => {
          done(e);
        });
    });

    it('should catch the invalid float type', (done) => {
      const cxnp = cxni.connect();
      const writep = cxnp.then(() => cxni.write([dpFlt]));
      const flushp = writep.then(() => cxni.flush());
      // catch invalid type in buffer
      writep.catch((e) => {
        console.log(JSON.stringify(e));
        done();
      });

      flushp.then(() => {
        done(new Error('Managed to write value of type Float to field of type Integer'));
      });
      flushp.catch((e) => {
        assert(e.message !== undefined);
        assert(e.data !== undefined);
        done();
      });
      // connection issue
      cxnp.catch((e) => {
        done(e);
      });
    });

    it('should drop the integer data', (done) => {
      done(util.dropMeasurement(cxni, 'pulse'));
    });
  });

  describe('#Conflict float to boolean', () => {
    const cxnb = new InfluxDB.Connection({
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

    const dpBool = {
      measurement: 'connection',
      timestamp: new Date().getTime() + 1000000,
      tags: [{ key: 'location', value: 'Turbine0017' }],
      fields: [{ key: 'connected', value: true }],
    };

    const dpFlt = {
      measurement: 'connection',
      timestamp: new Date().getTime() + 1000000,
      tags: [{ key: 'location', value: 'Turbine0017' }],
      fields: [{ key: 'connected', value: 0.0012 }],
    };

    it('should write the initial bool', (done) => {
      cxnb.connect()
        .then(() => cxnb.write([dpBool]))
        .then(() => cxnb.flush())
        .then(() => {
          done();
        })
        .catch((e) => {
          done(e);
        });
    });

    it('should catch the invalid float type', (done) => {
      const cxnp = cxnb.connect();
      const writep = cxnp.then(() => cxnb.write([dpFlt]));
      const flushp = writep.then(() => cxnb.flush());

      writep.catch((e) => {
        console.log(JSON.stringify(e));
        done();
      });

      flushp.then(() => {
        done(new Error('Managed to write value of type Float to field of type Boolean'));
      });
      flushp.catch((e) => {
        assert(e.message !== undefined);
        assert(e.data !== undefined);
        done();
      });
      // connection issue
      cxnp.catch((e) => {
        done(e);
      });
    });

    it('should drop the boolean data', (done) => {
      done(util.dropMeasurement(cxnb, 'connection'));
    });
  });

  describe('#Conflict float to string', () => {
    const cxns = new InfluxDB.Connection({
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

    const dpStr = {
      measurement: 'status',
      timestamp: new Date().getTime() + 1000000,
      tags: [{ key: 'location', value: 'Turbine0017' }],
      fields: [{ key: 'state', value: 'WARNING' }],
    };

    const dpFlt = {
      measurement: 'status',
      timestamp: new Date().getTime() + 1000000,
      tags: [{ key: 'location', value: 'Turbine0017' }],
      fields: [{ key: 'state', value: 7 }],
    };

    it('should write the initial string', (done) => {
      cxns.connect()
        .then(() => cxns.write([dpStr]))
        .then(() => cxns.flush())
        .then(() => {
          done();
        })
        .catch((e) => {
          done(e);
        });
    });

    it('should catch the invalid float type', (done) => {
      const cxnp = cxns.connect();
      const writep = cxnp.then(() => cxns.write([dpFlt]));
      const flushp = cxnp.then(() => cxns.flush());

      writep.catch((e) => {
        console.log(JSON.stringify(e));
        done();
      });

      flushp.then(() => {
        done(new Error('Managed to write value of type Float to field of type String'));
      });
      flushp.catch((e) => {
        assert(e.message !== undefined);
        assert(e.data !== undefined);
        done();
      });
      // connection issue
      cxnp.catch((e) => {
        done(e);
      });
    });

    it('should drop the string data', (done) => {
      done(util.dropMeasurement(cxns, 'status'));
    });
  });
});
