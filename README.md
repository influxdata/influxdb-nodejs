# InfluxDB Javascript Client #

This repository contains the javascript client of the InfluxDB time series database server from [InfluxData](https://www.influxdata.com/).  This client is officially supported by InfluxData.

The documentation here only covers the client API in Javascript.  For more information about the InfluxDB server please consult the [server documentation](https://docs.influxdata.com/influxdb/latest/introduction/).

### Quick Install ###

To quickly add the InfluxDB javascript client to your Node.js project:

    $ npm install --save influxdb-nodejs

### Overview ###

A full featured client written in ECMAScript 2015.

* Supports write batching and query result post-processing
* Supports Node.js 0.10 and later (note that all the examples are written using ES6 which is supported by Node.js natively since version 4)
* Provides easy access to the core InfluxDB API.

### Setup ###

You may [check the reference documentation as well.](https://dubsky.bitbucket.io/typedef/index.html#static-typedef-InfluxDB)

#### Connect to a database

```javascript
    const InfluxDB = require('influxdb-nodejs');

    const connection = new InfluxDB.Connection({
        hostUrl: 'http://localhost:8086',
        database: 'mydb'
    });

    connection.connect().then(() => {
       console.log('Connection established successfully');
    }).catch((e) => {
       console.error('Unexpected Error',e);
    });
```
#### Write to a database

```javascript
    connection.connect().then(() => {

        const dataPoint1 = {
            measurement : 'outdoorThermometer',
            timestamp: new Date(),
            tags: {
                location: 'greenhouse' },
                fields: { temperature: 23.7 }
        };

        // you can also provide tag and field data as arrays of objects:
        const dataPoint2 = {
            measurement : 'outdoorThermometer',
            timestamp: new Date().getTime()+1000000,
            tags: [
                {
                    key: 'location',
                    value: 'outdoor'
                }
            ],
            fields: [
                {
                    key: 'temperature',
                    value: 23.7
                }
            ]
        };

        const series = [dataPoint1, dataPoint2];
        connection.write(series).catch(console.error);

        connection.flush().then(() => {
            console.log('Data written into InfluxDB')
        }).catch(console.error);

    }).catch(console.error);
```

#### Read from a database

```javascript
    connection.connect().then(() => {
        connection.executeQuery('select * from outdoorThermometer group by location').then((result) => {
            console.log(result);
        }).catch(console.error);

    }).catch(console.error);
```

#### Drop data from a database

```javascript
    let connection = new InfluxDB.Connection({
        hostUrl: 'http://localhost:8086',
        database: 'mydb'
    });

    connection.connect().then(()=>{
        connection.executeQuery('drop measurement outdoorThermometer').then(() => {
          console.log('Measurement dropped');
        }).catch(console.error);

    }).catch(console.error);
```

[//]: # (* Summary of set up)
[//]: # (* Configuration )
[//]: # (* Dependencies)
[//]: # (* Database configuration)
[//]: # (* How to run tests)
[//]: # (* Deployment instructions)


### Releases ###

* Version 1.0.0 - Initial release
