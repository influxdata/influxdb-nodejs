/**
 * A ConnectionConfiguration must be provided when calling {@link Connection.create}
 * so that you can start writing data into InfluxDB.
 *
 * @typedef {Object} ConnectionConfiguration
 * @property {String} [username='root'] Username for connecting to the database.
 * @property {String} [password='root'] Password for connecting to the database.
 * @property {String} [database] Database to work with
 * @property {Boolean} [autoCreateDatabase=true] Automatically create database if it doesn't exist yet
 * @property {String} [hostUrl='http://127.0.0.1:8086'] InfluxDB host url to connect to. For InfluxCloud use https scheme
 *      and don't forget to pass username and password properties. For UDP access use a URL
 *      in the following form: udp://127.0.0.1:8089. For more information on using the UDP InfluxDB service check the
 *      documentation https://github.com/influxdata/influxdb/blob/master/services/udp/README.md
 * @property {Schema[]} [schema] schema of measurements accessed by this connection, validated when writing data points into
 *      InfluxDB. See {@link Schema} for more information.
 * @property {Boolean} [autoGenerateTimestamps=true] When writing data points without a timestamp:
 *   * If set to true the timestamp will be filled in automatically when {@link Connection.write} method is called
 *   * If false the timestamp will be filled in by the InfluxDB server
 * @property {Number} [batchSize=1000] Number of data points in a batch.
 *    Data points written into the InfluxDBConnection are
 *    buffered/cached in the connection until the batch size is
 *    reached or maximumWriteDelay is reached (see below.)
 *    To learn more about batching see {@link Connection.write}.
 * @property {Number} [maximumWriteDelay=1000] Maximum number of milliseconds
 *    the data point can be cached in the InfluxDBConnection.
 *    When this limit is reached for a single data point written, any older data points are also written into InfluxDB.
 *    To learn more about batching see {@link Connection.write}.
 * @property {Boolean} [autoResolveBufferedWritePromises=true] if true the {@link Promise} returned by
 * {@link Connection.write} will be automatically resolved when the data are put into the batching cache. This is
 *  useful for larger batch sizes for performance reasons and to avoid pollution of log files (there will be just one
 *  error generated for the whole batch on the last accepted {@link Connection.write} invocation).
 *  If false the {@link Promise} returned by {@link Connection.write} will always get rejected or resolved when
 *  the data points are written into InfluxDB. See more at {@link Connection.write}.
 * @property {function} [batchWriteErrorHandler] handler called when batch write to InfluxDB fails and the
 *  autoResolveBufferedWritePromises configuration property is set to true. This is useful in the case when the
 *  batch write gets triggered due to maximumWriteDelay expiration. See the example below:
 *
 *  To learn more about batching see {@link Connection.write}.
 *
 * @example
 * const InfluxDB = require('influx-nodejs')
 *
 * // Connect to a single host with a full set of config details and
 * // a custom schema
 * const connection = new InfluxDB.Connection({
 *   database: 'my_db',
 *   username: 'influxUser33',
 *   password: 'awesomeflux!',
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