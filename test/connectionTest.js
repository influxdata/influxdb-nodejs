let assert = require('assert');
let InfluxDB=require('../src/InfluxDB');
let util = require('./utils.js')

describe('Connection test', function(){

    describe('#Automatic connect and write', function(){

        let cxnaw = new InfluxDB.Connection({

            database: 'test1'

        })

        let dp1 = {
            measurement: 'location',
            timestamp: new Date(),
            tags: [{ key: 'unit', value: 'adam-12' }],
            fields: [{ key: 'coordinates' , value: '34_06_46_N_118_20_20_W'}]
        }

        let dp2 = {
            measurement: 'location',
            timestamp: new Date(),
            tags: [{ key: 'unit', value: 'l-30' }],
            fields: [{ key: 'coordinates' , value: '34.11856_N_118.30037_W'}]
        }

        let dp3 = {
            measurement: 'location',
            timestamp: new Date(),
            tags: [{ key: 'unit', value: 'zebra-07' }],
            fields: [{ key: 'coordinates' , value: '33_56_33_N_118_24_29_W'}]
        }

        it('should automatically connect and write data points, incl. timestamp', function(done){

            cxnaw.write([dp1,dp2, dp3], true).then(() => {
                done()
            }).catch((e) => {
                done(e)
            })

        })

        it('should read back and verify the data', function(done){

            cxnaw.executeQuery('Select * from location').then((result) => {
                console.log(result)
                assert.equal(result.length, 3)

                for(let dp of result){
                    assert( dp.time !== undefined);
                    switch(dp.unit){
                        case 'adam-12':
                            assert.equal(dp.coordinates, dp1.fields[0].value)
                            break;
                        case 'l-30':
                            assert.equal(dp.coordinates, dp2.fields[0].value)
                            break;
                        case 'zebra-07':
                            assert.equal(dp.coordinates, dp3.fields[0].value)
                            break;
                        default:
                            assert.fail(dp.unit, dp.coordinates, 'Unexpected element in results array');
                            break;
                    }
                }

                done()

            }).catch((e) => {
                done(e)
            })

        })

        it('should drop the measurement', function(done){
            result = util.dropMeasurement(cxnaw,'location'); //hmmm doesn't seem to be dropping

            //take a moment for transaction to complete otherwise lose connection object too soon in some cases
            util.sleep(1000).then(() => { done(result)});

/*
            cxnaw.connect().then(() => {

                cxnaw.executeQuery('DROP MEASUREMENT location').then(() => {
                    done()
                }).catch((e) => {
                    done(e);
                })

            }).catch((e) => {
                done(e);
            }); */
        })

    })

})