let assert = require('assert');
let InfluxDB=require('../src/InfluxDB');

describe('InfluxDB.Connection', function() {

    describe('#write(?,forceFlush=true)', function() {

        it('should write measurements to DB immediately', function (done) {

            let connection = new InfluxDB.Connection({
                database: 'test1'
            });

            connection.connect().then(() => {

                let f=function() {
                    let dataPoint1 = {
                        measurement: 'outdoorThermometerA',
                        timestamp: new Date(),
                        tags: {
                            location: 'greenhouse'
                        },
                        fields: {temperature: 23.7}
                    };

                    connection.write([dataPoint1], true).then(()=>{ console.log('written');}).catch((e) => {
                        done(e);
                    });

                    setTimeout( ()=>{
                        connection.executeQuery('select * from outdoorThermometerA').then((result) => {
                            assert.equal(result.length, 1);
                            console.log('assert evaluated');
                            done();
                        }).catch((e) => {
                            done(e);
                        });
                    }, 500);
                };

                connection.executeQuery('drop measurement outdoorThermometerA').then((result) => {
                    f();
                }).catch((e) => {
                    console.log('no measurement was there');
                    f();
                });

            }).catch((e) => {
                console.log('error', e);
                done(e);
            });

        });
    });

    describe('#write(?,forceFlush=false)', function() {

        it('should write measurements to DB with a delay', function (done) {

            let connection = new InfluxDB.Connection({
                database: 'test1'
            });

            connection.connect().then(() => {

                let f=function() {
                    let dataPoint1 = {
                        measurement: 'outdoorThermometerA',
                        timestamp: new Date(),
                        tags: {
                            location: 'greenhouse'
                        },
                        fields: {temperature: 23.7}
                    };

                    connection.write([dataPoint1]).then(()=>{ console.log('written');}).catch((e) => {
                        done(e);
                    });

                    connection.executeQuery('select * from outdoorThermometerA').then((result) => {
                        assert.equal(result.length, 0);
                        console.log('assert 1 evaluated');
                        setTimeout(()=>{
                            connection.executeQuery('select * from outdoorThermometerA').then((result) => {
                                assert.equal(result.length, 1);
                                console.log('assert 2 evaluated');
                                done();
                            }).catch((e) => {
                                done(e);
                            });
                        },1500);
                    }).catch((e) => {
                        done(e);
                    });
                };

                connection.executeQuery('drop measurement outdoorThermometerA').then((result) => {
                    f();
                }).catch((e) => {
                    console.log('no measurement was there');
                    f();
                });

            }).catch((e) => {
                console.log('error', e);
                done(e);
            });

        });
    });

    describe('#write(?,forceFlush=false)', function() {

        it('should write measurements to DB with a delay automatically', function (done) {

            let connection = new InfluxDB.Connection({
                database: 'test1'
            });

            connection.connect().then(() => {

                let f=function() {
                    let dataPoint1 = {
                        measurement: 'outdoorThermometerA',
                        timestamp: new Date(),
                        tags: {
                            location: 'greenhouse'
                        },
                        fields: {temperature: 23.7}
                    };

                    connection.write([dataPoint1]).then(()=>{ console.log('written');}).catch((e) => {
                        done(e);
                    });

                    connection.executeQuery('select * from outdoorThermometerA').then((result) => {
                        assert.equal(result.length, 0);
                        console.log('assert 1 evaluated');
                        setTimeout(()=>{
                            connection.executeQuery('select * from outdoorThermometerA').then((result) => {
                                assert.equal(result.length, 1);
                                console.log('assert 2 evaluated');
                                done();
                            }).catch((e) => {
                                done(e);
                            });
                        },1500);
                    }).catch((e) => {
                        done(e);
                    });
                };

                connection.executeQuery('drop measurement outdoorThermometerA').then((result) => {
                    f();
                }).catch((e) => {
                    console.log('no measurement was there');
                    f();
                });

            }).catch((e) => {
                console.log('error', e);
                done(e);
            });

        });
    });

    describe('#write(?,forceFlush=false)', function () {

        it('should write measurements to DB when cache size limit is reached;'+
            ' should output a console warning that not all data has been written', function (done) {

            let connection = new InfluxDB.Connection({
                database: 'test1'
            });

            connection.connect().then(() => {

                function buildPoints(base, count) {
                    let points = [];
                    for (let i = 0; i < count; i++) {
                        points.push({
                            measurement: 'outdoorThermometerA',
                            timestamp: base + i,
                            fields: {temperature: 23.7}
                        });
                    }
                    return points;
                }

                let f = function () {

                    connection.write(buildPoints(0,200)).then(() => {
                            console.log('written 200');

                            connection.executeQuery('select * from outdoorThermometerA').then((result) => {
                                assert.equal(result.length, 0);
                                console.log('assert 1 evaluated');
                                connection.write(buildPoints(1000, 900)).then(() => {
                                    console.log('written 900');
                                    // these should get written on the next round
                                    connection.write(buildPoints(2000, 10)).then(()=>{
                                        console.log('written 10');
                                    });
                                    setTimeout(()=>{
                                        connection.executeQuery('select * from outdoorThermometerA').then((result) => {
                                            assert.equal(result.length, 1100);
                                            console.log('assert 2 evaluated');
                                            done();
                                        }).catch((e) => {
                                            done(e);
                                        });
                                    }, 500);

                                }).catch((e) => {
                                    done(e);
                                });
                            }).catch((e) => {
                                done(e);
                            });
                        }
                    ).catch((e) => {
                        done(e);
                    });

                };

                connection.executeQuery('drop measurement outdoorThermometerA').then((result) => {
                    f();
                }).catch((e) => {
                    console.log('no measurement was there');
                    f();
                });

            }).catch((e) => {
                console.log('error', e);
                done(e);
            });

        });
    });

});