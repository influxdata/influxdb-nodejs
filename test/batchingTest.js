let assert = require('assert');
let InfluxDB = require('../src/InfluxDB');
const connections = require('./utils/connection.js');

describe('Check write functionality', () => {

    const connection = connections.getConnection('test1');

    describe('write forceFlush parameter', () => {
        it('#write(?,forceFlush=true) should write measurements to DB immediately', (done) => {
            connection.connect().then(() => {
                const onEmptyMeasurement = () => {
                    let dataPoint1 = {
                        measurement: 'outdoorThermometerA',
                        timestamp: new Date(),
                        tags: {
                            location: 'greenhouse'
                        },
                        fields: {temperature: 23.7}
                    };
                    connection.write([dataPoint1], true).then(() => {
                        console.log('written');
                    }, done);
                    setTimeout(() => {
                        connection.executeQuery('select * from outdoorThermometerA').then((result) => {
                            assert.equal(result.length, 1);
                            done();
                        }, done);
                    }, 500);
                };
                connection.executeQuery('drop measurement outdoorThermometerA').then(onEmptyMeasurement, onEmptyMeasurement);
            }, done);
        });

        it('#write(?,forceFlush=false) should write measurements to DB with a delay', (done) => {
            connection.connect().then(() => {
                const onEmptyMeasurement = () => {
                    let dataPoint1 = {
                        measurement: 'outdoorThermometerA',
                        timestamp: new Date(),
                        tags: {
                            location: 'greenhouse'
                        },
                        fields: {temperature: 23.7}
                    };
                    connection.write([dataPoint1]).then(() => {
                    }, done);
                    connection.executeQuery('select * from outdoorThermometerA').then((result) => {
                        assert.equal(result.length, 0);
                        console.log('assert 1 evaluated');
                        setTimeout(() => {
                            connection.executeQuery('select * from outdoorThermometerA').then((result) => {
                                assert.equal(result.length, 1);
                                console.log('assert 2 evaluated');
                                done();
                            }, done);
                        }, 1500);
                    }, done);
                };
                connection.executeQuery('drop measurement outdoorThermometerA').then(onEmptyMeasurement, onEmptyMeasurement);
            }, done);
        });

    });
});