/**
 * DataPoint can be passed into `Connection.write(dataPoint)` method
 * so that you can push data into the database. See: https://docs.influxdata.com/influxdb/v1.2/introduction/getting_started/#writing-and-exploring-data
 *
 * @public
 * @typedef {Object} DataPoint
 * @property {String} measurement Name of the measurement
 * @property {?(Date|String|Number)} timestamp=undefined timestamp of the measurement point. You can pass the following type of values:
 *   * Date - a javascript date object
 *   * Number - millisecond-precision Unix time
 *   * String - nanosecond-precision Unix time as a string. See https://github.com/sazze/node-nanotime if you need to generate these in node.js
 * @property {Tag[]|Object} tags Tags to be stored together with the datapoint
 * @property {Field[]|Object} fields Fields to be stored together with the datapoint
 *
 * @example
 *
 * let dataPoint1={
 *   measurement : 'outdoorThermometer',
 *   timestamp: '1465839830100400200',
 *   tags: { location: 'outdoor' },
 *   fields: { temperature: 23.7 }
 * }
 *
 * let dataPoint2={
 *   measurement : 'outdoorThermometer',
 *   timestamp: '1465839830100400200',
 *   tags: [ { key: 'location', value: 'greenhouse' } ]
 *   fields: [ { key: 'temperature', value: 23.7 } ]
 * }
 *
 */
