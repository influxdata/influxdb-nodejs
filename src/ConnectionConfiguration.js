/**
 * A ConnectionConfiguration must be provided when calling {@link InfluxDBConnection.create}
 * so that you can start writing data into InfluxDB.
 *
 * @typedef {Object} ConnectionConfiguration
 * @property {String} [username='root'] Username for connecting to the database.
 * @property {String} [password='root'] Password for connecting to the database.
 * @property {String} [database] Database to work with
 * @property {Boolean} [autoCreateDatabase=true] Automatically create database if it doesn't exist yet
 * @property {String} [hostUrl='http://127.0.0.1:8086'] Influx host url to connect to. For influx cloud use https scheme
 *      and don't forget to pass username and password properties. For UDP access use URL
 *      in the following form: udp://127.0.0.1:8089. For more information on using UDP InfluxDB service check the
 *      documentation https://github.com/influxdata/influxdb/blob/master/services/udp/README.md
 * @property {Schema[]} [schema] of measurements accessed by this connection, validated when writing data points into
 *      InfluxDB
 * @property {Boolean} [autoGenerateTimestamps=true] When writing data points without timestamp:
 *   * if set to true the timestamp will be filled-in automatically when {@link InfluxDBConnection.write} method is called
 *   * if false the timestamp will be filled by the influx db server
 * @property {Number} [batchSize=1000] Number of data points in a batch.
 *    Data points written into the InfluxDBConnection are
 *    buffered/cached on in the connection until the batch size is
 *    reached or maximumWriteDelay is reached (see below.)
 * @property {Number} [maximumWriteDelay=1000] Maximum number of milliseconds
 *    the data point can be cached in InfluxDBConnection.
 *    When this limit is reached for a single data point written, any older data points are also written into Influx DB
 * @property {Boolean} [autoResolvePromisedWritesToCache=true] if true the {@link Promise} returned by
 * {@link InfluxDBConnection.write} will be automatically resolved when the data are put into the batching cache. This is
 *  useful for larger batch sizes for performance reasons and to avoid pollution of log files (there will be just one
 *  error generated for the whole batch on the last accepted {@link InfluxDBConnection.write} invocation).
 *  If false the {@link Promise} returned by {@link InfluxDBConnection.write} will get always rejected or resolved when
 *  the data points are written into InfluxDB.
 * @property {function} [batchWriteErrorHandler] handler called when batch write into InfluxDB fails and
 *  autoResolvePromisedWritesToCache configuration property is set to true. This is useful in the case when the
 *  batch write gets triggered due to maximumWriteDelay expiration. See the example below:
 *
 * @example
 * import { InfluxDB } from 'influx-nodejs'; // or const InfluxDB = require('influx-nodejs')
 *
 * // Connect to a single host with a full set of config details and
 * // a custom schema
 * const connection = new InfluxDB.Connection({
 *   database: 'my_db',
 *   username: 'connor',
 *   password: 'pa$$w0rd',
 *   hostUrl: 'http://db1.example.com:8086' },
 *   schema: [{
 *     measurement: 'serverLoad',
 *     tags: ['hostname'],
 *     fields: {
 *       memory_usage: InfluxDB.FieldType.INTEGER,
 *       cpu_usage: InfluxDB.FieldType.FLOAT,
 *       is_online: InfluxDB.FieldType.BOOLEAN,
 *     }
 *   }],
 *   batchWriteErrorHandler(e, dataPoints) { // this is the same as the default value
 *     console.log('[DefaultBatchWriteErrorHandler] Error writing data points into InfluxDB:'+dataPoints,e);
 *   }
 * })
 */