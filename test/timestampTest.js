let assert = require('assert');
let InfluxDB=require('../src/InfluxDB');

describe('InfluxDB.timestamps', function(){

  describe('#timestamps check', function(){

    let testDate = new Date();

    // Timestamp type number
    let dpTS1 = {
         measurement: 'powerts',
         timestamp: testDate.getTime(),
         tags: [{ key: 'location', value: 'Turbine003' }],
         fields: [{ key: 'kwatts', value: 47 }]
    };

    // Timestamp type object
    let dpTS2 = {
        measurement: 'powerts',
        timestamp: testDate,
        tags: [{ key: 'location', value: 'Turbine005' }],
        fields: [{ key: 'kwatts', value: 46.5 }]
    };

    // Timestamp type String
    let dpTS3 = {
        measurement: 'powerts',
        timestamp: testDate, //.toISOString(),
        tags: [{ key: 'location', value: 'Turbine007'}],
        fields: [{ key: 'kwatts', value: 48.33 }]
    };


    let connection = new InfluxDB.Connection({
         database: 'test1'
    });


      it('should store datapoint with same timestamps', function(done) {

          connection.connect().then(() => {

               connection.write([dpTS1, dpTS2, dpTS3]).then(() => {
                   connection.flush().then(() => {
                       done();
                   }).catch((e) => {
                       done(e);
                   });
               }).catch((e) => {
                  done(e);
               })

            }).catch((e) => {
                 done(e);
            });

      });

      it('should read back the same timestamps', function(done){

        connection.connect().then(() => {

              connection.executeQuery('SELECT * FROM powerts').then((result) => {
                  assert.equal(result.length, 3);
                  for(let dp of result){
                     let td = new Date(dp.time);
                     assert( td.getTime() == testDate.getTime(), 'Returned time ' + td +
                              ' does not equal original test time ' + testDate +
                               ' ' + dp.location);
                     //assert((td.getTime() > (testDate.getTime() - 2000)) &&  ((td.getTime() < testDate.getTime() + 2000)),
                    //    "Returned time " + td + " is not within 2ms of test time " + testDate)
                  }
                  done();
              }).catch((e) => {
                  done(e);
              })
          }).catch((e) => {
              done(e);
          })
      });

      it('should drop the test datapoints', function(done) {

        connection.connect().then(() => {
               connection.executeQuery('DROP measurement powerts').then(() => {
                   done();
               }).catch((e) => {
                  done(e);
               })
          }).catch((e) => {
               done(e);
          })
      });
  })

});
