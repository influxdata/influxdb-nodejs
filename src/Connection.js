//import InfluxDBConnectionImpl from './impl/InfluxDBConnectionImpl';
let ConnectionImpl=require('./impl/ConnectionImpl');


/**
 * Main class used to communicate with InfluxDB
 *
 * @public
 */
class Connection {

    /**
     * Create new Connection object. To verify that everything is set correctly,
     * call the {@link Connection#connect} method
     *
     * @param {ConnectionConfiguration} options - all settings needed to connect
     * to InfluxDB and configure the communication protocol
     * @returns new {@link Connection} object
     */
    constructor(options) {
        this.stub=new ConnectionImpl(options);
    }

    /**
     * Verify the connection to InfluxDB is available. If you don't call this method on your own, it will be
     * called automatically before the first write to InfluxDB.
     *
     * @returns {Promise}
     * @throws {InfluxDBError}
     */
    connect() {
        return this.stub.connect();
    }

    /**
     * Write measurement data points into InfluxDB.
     *
     * By default data submitted for writing is buffered on the {@link Connection} object, to be written as a batch.
     * This buffer gets flushed and batch-written to InfluxDB when one of the following conditions are met:
     *
     *    1. Once its data point capacity has been reached (It is configured using
     *       {@link ConnectionConfiguration}.batchSize when creating the connection)
     *    2. Once the oldest data point submitted in the buffer gets older than
     *       the value {@link ConnectionConfiguration}.maximumWriteDelay defined when
     *       creating the connection.
     *    3. When you call the {@link Connection.flush} method
     *    4. When you call the {@link Connection.write} method with the optional parameter forceFlush=true
     *
     *
     * You may disable the batch writes by setting {@link ConnectionConfiguration}.batchSize or
     * {@link ConnectionConfiguration}.maximumWriteDelay to 0.
     * If batch writes are disabled, writes to InfluxDB will be initiated at each invocation of {@link Connection.write}.
     *
     * The function returns a promise. There are two ways promises are resolved
     * (distinguished by {@link ConnectionConfiguration}.autoResolvePromisedWritesToCache):
     *
     *    1. (autoResolvePromisedWritesToCache=true, Default) The promise gets resolved when the data points are stored
     *       in the connection buffer. In this case, some writes are stored in the buffer only to be batch-written.
     *       If an error occurs during communication with InfluxDB the error will propagate as follows:
     *       * To the write method promise that triggered communication with InfluxDB (either data point capacity
     *       overrun or by calling write with the parameter forceFlush=true)
     *       * To the promise returned by invoking {@link Connection.flush}
     *       * To the error handler defined by {@link ConnectionConfiguration}.batchWriteErrorHandler in the case the
     *         communication to InfluxDB is triggered when there is data in the connection
     *         buffer older than the delay defined by {@link ConnectionConfiguration}.maximumWriteDelay
     *    2. (autoResolvePromisedWritesToCache=false) The promise is never resolved before the data points are
     *         successfully written into InfluxDB.
     *         In the case of communication failure you will receive as many errors as the number of
     *         invocations of {@link ConnectionConfiguration.write} since the last successful write to InfluxDB.
     *         This mode is useful when you need finer visibility into the writes to InfluxDB (you may respond accordingly
     *         to each missed write). On the other hand applications that don't require finer visibility
     *         would suffer from log pollution from error messages (one error for each batch write to InfluxDB might
     *         be enough).
     *         Also, this mode consumes more cpu and memory resources.
     *
     *
     * @param {DataPoint[]} dataPoints - an array of measurement points to write to InfluxDB
     * @param {Boolean} [forceFlush=false] - if true the internal data point cache gets flushed into InfluxDB right away.
     * @returns {Promise} - a promise that is evaluated when data is either written into InfluxDB or
     * an i/o error occurs.
     * @throws {InfluxDBError}
     *
     * @example
     *   const series=[dataPoint1, dataPoint2];
     *   connection.write(series).then(() => {
     *        console.log('Data were written');
     *   }).catch(console.error);
     *
     */
    write(dataPoints, forceFlush) {
        return this.stub.write(dataPoints, forceFlush);
    }

    /**
     * Flush buffered points into InfluxDB server(s)
     * @returns {Promise}
     * @throws {InfluxDBError}
     */
    flush() {
        return this.stub.flush();
    }

    /**
     * Execute query on InfluxDB
     * @param {String} query text definition of the query, e.g. 'select * from outdoorTemperature'.
     * @returns {Array} post-processed data so that these are easy to work with. The result is an array of objects where
     * fields and tags are stored as regular properties. There is also a time property holding the
     * timestamp of the measurement as a JavaScript Date object.
     * @throws {InfluxDBError}
     *
     * @example
     * [
     *   {
     *     "time": "1970-01-18T07:59:02.227Z",
     *     "temperature": 23.7,
     *     "location": "greenhouse"
     *   },
     *   {
     *     "time": "1970-01-18T07:59:02.265Z",
     *     "temperature": 23.7,
     *     "location": "greenhouse"
     *   }
     * ]
     */
    executeQuery(query) {
        return this.stub.executeQuery(query);
    }

    /**
     * Execute query on InfluxDB and get unmodified JSON data result
     * @param {String} query text definition of the query, e.g. 'select * from outdoorTemperature'
     * @returns {Array} unmodified JSON response data
     * @throws {InfluxDBError}
     */
    executeRawQuery(query) {
        return this.stub.executeRawQuery(query);
    }

}

//export default InfluxDBConnection;

module.exports=Connection;