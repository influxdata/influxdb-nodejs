/**
 * DataPoint can be passed into `Connection.write(dataPoint)` method
 * so that you can write points to the database.
 * See: https://docs.influxdata.com/influxdb/latest/introduction/getting_started/#writing-and-exploring-data
 *
 * @public
 * @typedef {Object} DataPoint
 * @property {String} measurement Name of the measurement
 * @property {?(Date|String|Number)} timestamp=undefined timestamp of the measurement point. You can pass the following
 *   type of values:
 *   * Date - a JavaScript date object (it is millisecond-precision)
 *   * Number - millisecond-precision Unix time
 *   * String - nanosecond-precision Unix time as a string. See https://github.com/sazze/node-nanotime if you need to
 *            generate these in Nodejs
 * @property {Tag[]|Object} tags Tags to be stored together with the data point. Two formats are supported:
 *    * either an array of objects with key/value properties (see the example below) or an object where it's property
 *      keys serve as tag keys and object property values as tag values
 * @property {Field[]|Object} fields Fields to be stored together with the data point. Two formats are supported:
 *    * either an array of objects with key/value properties (see the example below) or an object where it's property
 *      keys serve as field keys and object property values as field values
 *
 * @example
 * let dataPoint1={
 *   measurement : 'outdoorThermometer',
 *   timestamp: '1465839830100400200',
 *   tags: { location: 'outdoor' },
 *   fields: { temperature: 18.3 }
 * }
 *
 * let dataPoint2={
 *   measurement : 'outdoorThermometer',
 *   timestamp: '1465839831270501600',
 *   tags: [ { key: 'location', value: 'greenhouse' } ]
 *   fields: [ { key: 'temperature', value: 23.7 } ]
 * }
 *
 */
