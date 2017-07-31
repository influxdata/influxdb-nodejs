/* global describe it */
const assert = require('assert');
const InfluxDB = require('../src/InfluxDB');
const util = require('../scripts/utils.js');

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
      connection.connect().then(() => {
        connection.write([dPF1]).then(() => {
          connection.flush().then(() => {
            done(new Error('tags are not properly checked'));
          }).catch((e) => {
            console.log('ERROR ON FLUSH');
            done(e);
          });
        }).catch(() => {
          done();
        });
      }).catch((e) => {
        done(e);
      });
    });

    it('should not fail when no tags specified', (done) => {
      const dPF1 = {
        measurement: 'powerf2',
        timestamp: new Date().getTime() + 1000000,
        fields: [{ key: 'kwatts', value: 49 }],
      };
      connection.connect().then(() => {
        connection.write([dPF1]).then(() => {
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

      connection.connect().then(() => {
        connection.write([dPF1, dPF2, dPF3]).then(() => {
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

    it('should verify the field type in Influx', (done) => {
      connection.connect().then(() => {
        connection.executeQuery('SHOW FIELD KEYS').then((result) => {
          assert.equal(util.getFieldType(result, 'kwatts'), 'float');
          done();
        }).catch((e) => {
          done(e);
        });
      });
    });

    it('should read back the float values', (done) => {
      connection.connect().then(() => {
        connection.executeQuery('select * from powerf').then((result) => {
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
        }).catch((e) => {
          done(e);
        });
      }).catch((e) => {
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

      connection.connect().then(() => {
        connection.write([dPI1, dPI2, dPI3]).then(() => {
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

    it('should verify the field type in Influx', (done) => {
      connection.connect().then(() => {
        connection.executeQuery('SHOW FIELD KEYS').then((result) => {
          assert.equal(util.getFieldType(result, 'rpms'), 'integer');
          done();
        }).catch((e) => {
          done(e);
        });
      });
    });

    it('should read back the integer values', (done) => {
      connection.connect().then(() => {
        connection.executeQuery('select * from testint').then((result) => {
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
        }).catch((e) => {
          done(e);
        });
      }).catch((e) => {
        done(e);
      });
    });

    it('should drop the integer values', (done) => {
      done(util.dropMeasurement(connection, 'testint'));
    });
  });

  describe('#Strings', () => {
    const jsonTestStr = '{ type: \'widget\', name: \'kralik\', id: 123456789 }';
    const qlTestStr = 'SELECT * FROM teststr';

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
      fields: [{ key: 'status', value: jsonTestStr }],
    };

    const dpS3 = {
      measurement: 'teststr',
      timestamp: new Date(),
      tags: [{ key: 'location', value: 'Turbine0019' }],
      fields: [{ key: 'status', value: qlTestStr }],
    };

    it('should write strings', (done) => {
      connection.connect().then(() => {
        connection.write([dpS1, dpS2, dpS3]).then(() => {
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

    it('should verify the field type in Influx', (done) => {
      connection.connect().then(() => {
        connection.executeQuery('SHOW FIELD KEYS').then((result) => {
          assert.equal(util.getFieldType(result, 'status'), 'string');
          done();
        }).catch((e) => {
          done(e);
        });
      });
    });


    it('should read the strings back', (done) => {
      connection.connect().then(() => {
        connection.executeQuery('select * from teststr').then((result) => {
          assert.equal(result.length, 3);
          result.forEach((dp) => {
            switch (dp.location) {
              case 'Turbine0017':
                assert.equal(dp.status, 'OK');
                break;
              case 'Turbine0018':
                assert.equal(dp.status, jsonTestStr);
                break;
              case 'Turbine0019':
                assert.equal(dp.status, qlTestStr);
                break;
              default:
                assert.fail(dp.status, dp.location,
                  'unexpected element in results array', ',');
                break;
            }
          });
          done();
        }).catch((e) => {
          done(e);
        });
      }).catch((e) => {
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
      connection.connect().then(() => {
        connection.write([dpB1, dpB2, dpB3, dpB4, dpB5]).then(() => {
//                        done()
        }).catch((e) => {
          done(e);
        });

        connection.flush().then(() => {
          done();
        }).catch((e) => {
          done(e);
        });
      }).catch((e) => {
        done(e);
      });
    });

    it('should verify the field type in Influx', (done) => {
      connection.connect().then(() => {
        connection.executeQuery('SHOW FIELD KEYS').then((result) => {
          assert.equal(util.getFieldType(result, 'online'), 'boolean');
          done();
        }).catch((e) => {
          done(e);
        });
      });
    });


    it('should read the booleans back', (done) => {
      connection.connect().then(() => {
        connection.executeQuery('select * from testbool').then((result) => {
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
        }).catch((e) => {
          done(e);
        });
      }).catch((e) => {
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
      cxnf.connect().then(() => {
        cxnf.write([dpFlt]).then(() => {

        }).catch((e) => {
          done(e);
        });

        cxnf.flush().then(() => {
          done();
        }).catch((e) => {
          done(e);
        });
      }).catch((e) => {
        done(e);
      });
    });

    it('should catch the invalid string type', (done) => {
      cxnf.connect().then(() => {
        cxnf.write([dpStr]).catch(() => {
    //                done(e)
        });

        cxnf.flush().then(() => {
          done(new Error('Managed to write value of type String to field of type Float'));
        }).catch((e) => {
//                    console.log('second write flush failed - which is correct: ' + e);
          assert(e.message !== undefined);
          assert(e.data !== undefined);
          done();
        });
      }).catch((e) => {
        done(e);
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
      cxni.connect().then(() => {
        cxni.write([dpInt]).then(() => {

        }).catch((e) => {
          done(e);
        });

        cxni.flush().then(() => {
          done();
        }).catch((e) => {
          done(e);
        });
      }).catch((e) => {
        done(e);
      });
    });

    it('should catch the invalid float type', (done) => {
      cxni.connect().then(() => {
        cxni.write([dpFlt]).catch(() => {
                    //                done(e)
        });

        cxni.flush().then(() => {
          done(new Error('Managed to write value of type Float to field of type Integer'));
        }).catch((e) => {
          assert(e.message !== undefined);
          assert(e.data !== undefined);
          done();
        });
      }).catch((e) => {
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
      cxnb.connect().then(() => {
        cxnb.write([dpBool]).then(() => {

        }).catch((e) => {
          done(e);
        });

        cxnb.flush().then(() => {
          done();
        }).catch((e) => {
          done(e);
        });
      }).catch((e) => {
        done(e);
      });
    });

    it('should catch the invalid float type', (done) => {
      cxnb.connect().then(() => {
        cxnb.write([dpFlt]).catch(() => {
                    //                done(e)
        });

        cxnb.flush().then(() => {
          done(new Error('Managed to write value of type Float to field of type Boolean'));
        }).catch((e) => {
//                    console.log('second write flush failed - which is correct: ' + e);
          assert(e.message !== undefined);
          assert(e.data !== undefined);
          done();
        });
      }).catch((e) => {
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
      cxns.connect().then(() => {
        cxns.write([dpStr]).then(() => {

        }).catch((e) => {
          done(e);
        });

        cxns.flush().then(() => {
          done();
        }).catch((e) => {
          done(e);
        });
      }).catch((e) => {
        done(e);
      });
    });

    it('should catch the invalid float type', (done) => {
      cxns.connect().then(() => {
        cxns.write([dpFlt]).catch(() => {
                    //                done(e)
        });

        cxns.flush().then(() => {
          done(new Error('Managed to write value of type Float to field of type Boolean'));
        }).catch((e) => {
//                    console.log('second write flush failed - which is correct: ' + e);
          assert(e.message !== undefined);
          assert(e.data !== undefined);
          done();
        });
      }).catch((e) => {
        done(e);
      });
    });

    it('should drop the string data', (done) => {
      done(util.dropMeasurement(cxns, 'status'));
    });
  });
});
