let assert = require('assert')
let InfluxDB = require('../src/InfluxDB');


describe('InfluxDB.types', function () {

    let connection = new InfluxDB.Connection({
        database: 'test1',
        schema: [
            {
                measurement: 'powerf',
                tags: ['location'],
                fields: {
                    kwatts: InfluxDB.FieldType.FLOAT
                }
            },
            {
                measurement: 'testint',
                tags: ['location'],
                fields: {
                    rpms: InfluxDB.FieldType.INTEGER
                }
            }

        ]

    });

    describe('#floats', function () {

        it('should write floats in all formats', function (done) {
            let dPF1 = {
                measurement: 'powerf',
                timestamp: new Date().getTime() + 1000000,
                tags: [{key: 'location', value: 'Turbine0017'}],
                fields: [{key: 'kwatts', value: 49}]
            };

            let dPF2 = {
                measurement: 'powerf',
                timestamp: new Date().getTime() + 1000000,
                tags: [{key: 'location', value: 'Turbine0018'}],
                fields: [{key: 'kwatts', value: 49.013}]
            };

            let dPF3 = {
                measurement: 'powerf',
                timestamp: new Date().getTime() + 1000000,
                tags: [{key: 'location', value: 'Turbine0019'}],
                fields: [{key: 'kwatts', value: 5.009e+1}]
            };

            connection.connect().then(() => {

                connection.write([dPF1, dPF2, dPF3]).then(() => {
                    connection.flush().then(() => {
                        done();
                    }).catch((e) => {
                        console.log('ERROR ON FLUSH');
                        done(e);
                    });

                }).catch((e) => {
                    done(e);
                });

            }).catch((e) => {
                done(e);
            });
        });

        it('should read back the float values', function (done) {

            connection.connect().then(() => {

                connection.executeQuery('select * from powerf').then((result) => {
                    assert.equal(result.length, 3);
                    for (let dp of result) {
                        switch (dp.location) {
                            case 'Turbine0017': //49
                                assert.equal(dp.kwatts, 49.0);
                                break;
                            case 'Turbine0018': //49.013
                                assert.equal(dp.kwatts, 49.013);
                                break;
                            case 'Turbine0019': //5.009e+1
                                assert.equal(dp.kwatts, 50.09);
                                break;
                            default:
                                assert.fail(dp.kwatts, dp.location,
                                    'unexpected element in results array', ',');
                                break;
                        }
                    }
                    done()
                }).catch((e) => {
                    console.log('ERROR ON READ BACK');
                    done(e)
                });
            }).catch((e) => {
                done(e)
            });
        });

        it('should drop the float values', function (done) {

            connection.connect().then(() => {

                connection.executeQuery('drop measurement powerf').then((result) => {
                    done()
                }).catch((e) => {
                    console.log('DROP QUERY EXEC ERROR')
                    done(e)
                })

            }).catch((e) => {
                done(e)
            });

        })

    });

    /*
     N.B.  - Using reductio ad absurdum - writing integers as strings according to format specified
     in Line Protocol Reference

     https://docs.influxdata.com/influxdb/v1.2/write_protocols/line_protocol_reference/#syntax

     INSERT mymeas value=1i

     We should probably decide upon a type convention, perhaps export FIELD_TYPE

     */
    describe('#Integers', function () {
        it('should write integers', function (done) {
            let dPI1 = {
                measurement: 'testint',
                timestamp: new Date(),
                tags: [{key: 'location', value: 'Turbine0017'}],
                fields: [{key: 'rpms', value: 49}] //, type: FieldType.INTEGER}]
            };

            let dPI2 = {
                measurement: 'testint',
                timestamp: new Date(),
                tags: [{key: 'location', value: 'Turbine0018'}],
                fields: [{key: 'rpms', value: Number.MAX_SAFE_INTEGER}] //, type: FieldType.INTEGER}]
            };

            let dPI3 = {
                measurement: 'testint',
                timestamp: new Date(),
                tags: [{key: 'location', value: 'Turbine0019'}],
                fields: [{key: 'rpms', value: Number.MIN_SAFE_INTEGER}] //, type: FieldType.INTEGER}]
            };

            console.log([dPI1, dPI2, dPI3])
            connection.connect().then(() => {
                connection.write([dPI1, dPI2, dPI3]).then(() => {
                    done()
                }).catch((e) => {
                    done(e)
                });

            }).catch((e) => {
                done(e)
            });
//N.B. need to check writing a float value to a field already defined as int
        });

        it('should read back the integer values', function (done) {

            connection.connect().then(() => {

                connection.executeQuery('select * from testint').then((result) => {
                    //                      console.log(result)
                    assert.equal(result.length, 3);
                    for (let dp of result) {
                        switch (dp.location) {
                            case 'Turbine0017': //49
                                assert.equal(dp.rpms, 49);
                                break;
                            case 'Turbine0018': //MAX_INT
                                assert.equal(dp.rpms, Number.MAX_SAFE_INTEGER);
                                break;
                            case 'Turbine0019': //MIN_INT
                                assert.equal(dp.rpms, Number.MIN_SAFE_INTEGER);
                                break;
                            default:
                                assert.fail(dp.rpms, dp.location,
                                    'unexpected element in results array', ',');
                                break;
                        }
                    }
                    done()
                }).catch((e) => {
                    console.log('ERROR ON READ BACK');
                    done(e)
                });

            }).catch((e) => {
                done(e)
            });
        });

        it('should drop the integer values', function (done) {

            connection.connect().then(() => {

                connection.executeQuery('drop measurement testint').then((result) => {
                    done()
                }).catch((e) => {
                    console.log('DROP QUERY EXEC ERROR');
                    done(e)
                })

            }).catch((e) => {
                done(e)
            });
        })

    });

    describe('#Strings', function () {

        it('should write strings', function (done) {

            let jsonTestStr = '{ type: \'widget\', name: \'kralik\', id: 123456789 }';
            let qlTestStr = 'SELECT * FROM teststr';

            let dpS1 = {
                measurement: 'teststr',
                timestamp: new Date(),
                tags: [{key: 'location', value: 'Turbine0017'}],
                fields: [{key: 'status', value: 'OK'}]
            };

            let dpS2 = {
                measurement: 'teststr',
                timestamp: new Date(),
                tags: [{key: 'location', value: 'Turbine0018'}],
                fields: [{key: 'status', value: jsonTestStr}]
            };

            let dpS3 = {
                measurement: 'teststr',
                timestamp: new Date(),
                tags: [{key: 'location', value: 'Turbine0019'}],
                fields: [{key: 'status', value: qlTestStr}]
            };

            connection.connect().then(() => {

                connection.write([dpS1, dpS2, dpS3]).then(() => {
                    done()
                }).catch((e) => {
                    done(e)
                });

            }).catch((e) => {
                done(e)
            });

        });

        it('should read the strings back', function (done) {

            connection.connect().then(() => {

                connection.executeQuery('select * from teststr').then((result) => {
                    assert.equal(result.length, 3);
                    for (let dp of result) {
                        console.log("DEBUG " + dp.status);
                        switch (dp.location) {
                            case 'Turbine0017':
                                assert.equal(dp.status, 'OK');
                                break;
                            case 'Turbine0018':
                                assert.equal(dp.status, '{ type: \'widget\', name: \'kralik\', id: 123456789 }');
                                break;
                            case 'Turbine0019':
                                assert.equal(dp.status, 'SELECT * FROM teststr');
                                break;
                            default:
                                assert.fail(dp.status, dp.location,
                                    'unexpected element in results array', ',');
                                break;
                        }
                    }

                    done()
                }).catch((e) => {
                    console.log('ERROR ON READ BACK');
                    done(e)
                });

            }).catch((e) => {
                done(e)
            });

        });

        it('should drop the datapoints', function (done) {

            connection.connect().then(() => {
                connection.executeQuery('drop measurement teststr').then((result) => {
                    done()
                }).catch((e) => {
                    console.log('DROP QUERY EXEC ERROR');
                    done(e)
                })

            }).catch((e) => {
                done(e)
            });

        })

    })

});
