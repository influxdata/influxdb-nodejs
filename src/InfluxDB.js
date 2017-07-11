/**
 * This is the main import of the influxdb-node.js package.
 *
 * @public
 * @typedef {Object} InfluxDB
 * @property {Connection} Connection reference to the {@link Connection} object
 * @property {FieldType} FieldType reference to the {@link FieldType} object, used to define measurement schemas
 *
 * @example
 * const InfluxDB=require('influxdb-nodejs');
 *
 * const connection=new InfluxDB.Connection({
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
const FieldType = require('./Field').FieldType;
/**
 * @ignore
 */
const Connection = require('./Connection');
/**
 * @ignore
 */
const InfluxDBError = require('./InfluxDBError');

module.exports={ Connection, FieldType, InfluxDBError };