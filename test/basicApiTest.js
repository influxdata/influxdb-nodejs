let assert = require('assert');
let InfluxDB=require('../src/InfluxDB');

describe('InfluxDB.Connection', function() {
    describe('#connect()', function() {
        it('should connect to DB', function (done) {
            let connection = new InfluxDB.Connection({
                database: 'test1'
            });

            /* Test against cloud
             let connection = new InfluxDB.Connection({
             hostUrl: 'https://futureboy-bf9e2f8a.influxcloud.net:8086',
             username: 'admin',
             password: 'changeit@123',
             database: 'test1'
             });*/

            connection.connect().then((result) => {
                done();
            }).catch((e) => {
                console.log('error', e);
                done(e);
            });

        });

        it('should connect to DB with url ending with a slash', function (done) {
            let connection = new InfluxDB.Connection({
                database: 'test1',
                hostUrl: 'http://localhost:8086/'
            });

            connection.connect().then((result) => {
                done();
            }).catch((e) => {
                console.log('error', e);
                done(e);
            });

        });

    });

    describe('#write()', function() {

        it('should write measurements to DB', function (done) {

            let connection = new InfluxDB.Connection({
                database: 'test1'
            });

            connection.connect().then(() => {

                let dataPoint1 = {
                    measurement: 'outdoorThermometer',
                    timestamp: new Date(),
                    tags: {
                        location: 'greenhouse'
                    },
                    fields: {temperature: 23.7}
                };

                let dataPoint2 = {
                    measurement: 'outdoorThermometer',
                    timestamp: new Date().getTime() + 1000000,
                    tags: [{key: 'location', value: 'outdoor'}],
                    fields: [{key: 'temperature', value: 23.7}]
                };

                connection.write([dataPoint1, dataPoint2]).catch((e) => {
                    done(e);
                });
                connection.flush().then(() => {
                    done()
                }).catch((e) => {
                    done(e);
                });

            }).catch((e) => {
                console.log('error', e);
                done(e);
            });

        });
    });

    describe('#executeQuery()', function() {

        it('should read measurements from DB', function(done) {

            let connection=new InfluxDB.Connection({
                database: 'test1'
            });

            connection.connect().then(()=>{
                connection.executeQuery('select * from outdoorThermometer group by location').then((r) => {
                    done()
                }).catch((e) => {
                    done(e);
                });

            }).catch((e)=>{
                console.log('error',e);
                done(e);
            });

        });

        it('should drop measurements from DB', function(done) {

            let connection=new InfluxDB.Connection({
                database: 'test1'
            });

            connection.connect().then(()=>{
                connection.executeQuery('drop measurement outdoorThermometer').then((r) => {
                    done()
                }).catch((e) => {
                    done(e);
                });

            }).catch((e)=>{
                console.log('error',e);
                done(e);
            });
        });

    });
});