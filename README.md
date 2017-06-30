# InfluxDB Javascript Client #

This repository contains the javascript client of the InfluxDB time series database server from [Influxdata](https://www.influxdata.com/).  This client is officially supported by Influxdata.

The documentation here only covers the client API in Javascript.  For more inflormation about the InfluxDB server please consult the [server documentation](https://docs.influxdata.com/influxdb/v1.2/introduction/).

### Quick Install ###

To quickly add the InfluxDB javascript client to your Nodejs project:

    $ npm install --save influxdb-nodejs

### Overview ###

A full featured client written in ECMAScript 2015.

* Supports write batching and query result post-processing
* Supports nodejs 0.10 and later (note that all the examples are written using ES6 which is supported by node.js natively since node version 4)
* Provides easy access to the core InfluxDB API.


Version 1.0.0

### Setup ###

You may [check the reference documentation as well.](https://dubsky.bitbucket.io/typedef/index.html#static-typedef-InfluxDB)

#### Connect to a database
```JavaScript
    ...
    let InfluxDB=require('influxdb-nodejs');
    let connection=new InfluxDB.Connection({
        hostUrl: 'http://localhost:8086',
        database: 'mydb'
    });

    connection.connect().then((result)=>{
       console.log('connection success');
    }).catch((e)=>{
        console.log('error',e);
    });
    ...
```

#### Write to a database
```JavaScript
    ...

    connection.connect().then(()=>{

        let dataPoint1={
            measurement : 'outdoorThermometer',
            timestamp: new Date(),
            tags: {
                location: 'greenhouse' },
                fields: { temperature: 23.7 }
        };

        let dataPoint2={
            measurement : 'outdoorThermometer',
            timestamp: new Date().getTime()+1000000,
            tags: [ { key: 'location', value: 'outdoor' }],
            fields: [ { key: 'temperature', value: 23.7 } ]
        };

        connection.write([dataPoint1,dataPoint2]).catch((e) => {
          console.log('Error writing data point',e);
          });
        connection.flush().then(()=>{
          console.log('Error flushing write buffer')
          }).catch((e) => {
            console.log('Error', e);
          });

    }).catch((e)=>{
        console.log('error',e);
    });
    ...
```

#### Read from a database
```JavaScript
    ...

    connection.connect().then(()=>{
        connection.executeQuery('select * from outdoorThermometer group by location').then((r) => {
            console.log(r);
        }).catch((e) => {
            console.log('Error executing query',e);
        });

    }).catch((e)=>{
        console.log('error',e);
    });
    ...
```
#### Drop data from a database
```JavaScript
    ...
    let connection=new InfluxDB.Connection({
        hostUrl: 'http://localhost:8086',
        database: 'mydb'
    });

    connection.connect().then(()=>{
        connection.executeQuery('drop measurement outdoorThermometer').then((r) => {
          console.log(r);
        }).catch((e) => {
          console.log('Error executing query', e);
        });

    }).catch((e)=>{
        console.log('error',e);
    });
    ...
```
[//]: # (* Summary of set up)
[//]: # (* Configuration )
[//]: # (* Dependencies)
[//]: # (* Database configuration)
[//]: # (* How to run tests)
[//]: # (* Deployment instructions)

### Contribution guidelines ###

* Writing tests
* Code review
* Other guidelines

### Who do I talk to? ###

* Repo owner or admin
* Other community or team contact
