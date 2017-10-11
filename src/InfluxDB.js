/**
 * This is the main import of the influxdb-node.js package.
 *
 * @public
 * @typedef {Object} InfluxDB
 * @property {Connection} Connection reference to the {@link Connection} object
 * @property {FieldType} FieldType reference to the {@link FieldType} object, used to define
 *   measurement schemas
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
import FieldType from '~/Field';
/**
 * @ignore
 */
import Connection from '~/Connection';
/**
 * @ignore
 */
import InfluxDBError from '~/InfluxDBError';

export { Connection, FieldType, InfluxDBError };
