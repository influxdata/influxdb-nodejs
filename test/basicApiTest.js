const assert = require('assert');
const InfluxDB = require('../src/InfluxDB');
const connections = require('./utils/connection.js');


describe('Quick test of all main functions', () => {

    const connection = connections.getConnection('test1');

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

    describe('#connect()', () => {
        it('should connect to DB', (done) => {
            connection.connect().then(done, done);
        });

        it('should connect to DB with url ending with a slash', (done) => {
            let connection = new InfluxDB.Connection({
                database: 'test1',
                hostUrl: 'http://localhost:8086/'
            });
            connection.connect().then(done, done);
        });

    });

    describe('#write()', function () {
        it('should flush written measurements to DB (with promise auto resolving)', (done) => {
            connection.write([dataPoint1, dataPoint2]).then(() => {
                console.log('written');
            }, done);
            connection.flush().then(() => {
                console.log('flushed');
                done();
            }, done);
        });
    });

    describe('#executeQuery()', () => {
        it('should read measurements from DB', (done) => {
            connection.executeQuery('select * from outdoorThermometer group by location')
                .then((result) => {
                    console.log(result);
                    done();
                }, done);
        });
    });

    describe('#executeRawQuery()', () => {
        it('should read measurements from DB in raw format', (done) => {
            connection.executeRawQuery('select * from outdoorThermometer group by location')
                .then((result) => {
                    console.log(result);
                    done();
                }, done);
        });
    });

    describe('#disconnect()', () => {
        it('no action should be taken after disconnect from DB', (done) => {
            connection.disconnect();
            const p1=connection.executeQuery('select * from outdoorThermometer group by location')
                .then(() => { done(new Error('Should fail')); }).catch(()=>{});
            const p2=connection.executeRawQuery('select * from outdoorThermometer group by location')
                .then(() => { done(new Error('Should fail')); }).catch(()=>{});
            const p3=connection.write([dataPoint1])
                .then(() => { done(new Error('Should fail')); }).catch(()=>{});
            Promise.all([p1, p2, p3]).then(() => {
                done();
            }, done);

        });

        it('disconnect after disconnect won\'t upset us', (done) => {
            connection.disconnect().catch((e) => done(e));
            connection.disconnect().then(() => {
                connection.disconnect().then(done, done);
            }, done);
        });

        it('everything should work fine after reconnect', (done) => {
            connection.disconnect().then(() => {
                connection.connect().then(() => {
                    const p1 = connection.executeQuery('select * from outdoorThermometer group by location').then((result) => {
                        console.log(result.length);
                    }, done);
                    const p2 = connection.executeRawQuery('select * from outdoorThermometer group by location').then(() => {}, done);
                    const p3 = connection.write([dataPoint1]).then(() => {}, done);
                    Promise.all([p1, p2, p3]).then(() => {
                        done();
                    }, done);
                }, done)
            }, done);
        });
        it('should drop measurements from DB', (done) => {
            connection.flush().then(()=> {
                connection.executeQuery('drop measurement outdoorThermometer').then(()=>{ done(); },done);
            }, done);
        });
    });

});