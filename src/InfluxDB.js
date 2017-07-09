/**
 * This is the main import of the influxdb-node.js package.
 *
 * @public
 * @typedef {Object} InfluxDB
 * @property {Connection} Connection reference to the {@link Connection} object
 * @property {FieldType} FieldType reference to the {@link FieldType} object, used to define measurement schemas
 *
 * @example
 *
 * const InfluxDB=require('influxdb-nodejs');
 *
 * let connection=new InfluxDB.Connection({
 *       hostUrl: 'http://localhost:8086',
 *       database: 'mydb'
 * });
 *
 * ...
 *
 */

/**
 * @ignore
 */
let FieldType = require('./Field').FieldType;
/**
 * @ignore
 */
let Connection = require('./Connection');
/**
 * @ignore
 */
let InfluxDBError = require('./InfluxDBError');

module.exports={ Connection, FieldType, InfluxDBError };