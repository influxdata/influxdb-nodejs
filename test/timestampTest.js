/* global describe it */
import assert from 'assert';
import * as InfluxDB from '~/InfluxDB';

describe('Check timestamp handling', () => {
    describe('timestamps check', () => {
        const testDate = new Date();

        // Timestamp type number
        const dataPoint1 = {
            measurement: 'powerts',
            timestamp: testDate.getTime(),
            tags: [{key: 'location', value: 'Turbine003'}],
            fields: [{key: 'kwatts', value: 47}],
        };

        // Timestamp type object
        const dataPoint2 = {
            measurement: 'powerts',
            timestamp: testDate,
            tags: [{key: 'location', value: 'Turbine005'}],
            fields: [{key: 'kwatts', value: 46.5}],
        };

        // Timestamp type String
        const dataPoint3 = {
            measurement: 'powerts',
            timestamp: testDate,
            tags: [{key: 'location', value: 'Turbine007'}],
            fields: [{key: 'kwatts', value: 48.33}],
        };

        const connection = new InfluxDB.Connection({
            database: 'test1',
        });

        it('should store datapoint with same timestamps', (done) => {
            connection.write([dataPoint1, dataPoint2, dataPoint3]).then(() => {
                connection.flush().then(done, done);
            });
        });

        it('should read back the same timestamps', (done) => {
            connection.executeQuery('SELECT * FROM powerts').then((result) => {
                assert.equal(result.length, 3);
                result.forEach((element) => {
                    const td = new Date(element.time);
                    assert(td.getTime() === testDate.getTime(), `Returned time ${td}
                              does not equal original test time ${testDate} ${element.location}`);
                });
                done();
            }, done);
        });

        it('should handle undefined timestamps, autoGenerateTimestamps=false', (done) => {
            connection.write({
                measurement: 'powerts',
                tags: [{key: 'location', value: 'Turbine00312'}],
                fields: [{key: 'kwatts', value: 47}],
            }).then(done, done);
        });


        it('should handle undefined timestamps,autoGenerateTimestamps=true', (done) => {
            connection.write({
                measurement: 'powerts',
                tags: [{key: 'location', value: 'Turbine00312'}],
                fields: [{key: 'kwatts', value: 47}],
            }).then(done, done);
        });


        it('should drop the test datapoints', (done) => {
            connection.flush().then(()=>{
                connection.executeQuery('DROP measurement powerts').then(() => {
                    done();
                }, done);
            });
        });
    });
});
